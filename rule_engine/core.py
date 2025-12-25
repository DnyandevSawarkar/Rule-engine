"""
Core Rule Engine implementation
"""

import json
import os
from datetime import datetime, date, timezone
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any
from loguru import logger

from .models import CouponData, ContractData, ContractAnalysis, ProcessingResult
from .exceptions import RuleEngineError, ValidationError, ContractError
from .contract_loader import ContractLoader
from .rule_loader import RuleLoader
from .eligibility_checker_v2 import EligibilityCheckerV2
from .computation_engine import ComputationEngine
from .addon_processor import AddonRuleProcessor


def get_rule_engine_version() -> str:
    """Get the current rule engine version"""
    # Always return the current library version
    # This ensures consistency regardless of installed package versions
    return "1.2.0"


class RuleEngine:
    """
    Main Rule Engine class for processing airline coupons against contracts
    """
    
    def __init__(self, contracts_dir: str = "contracts", rules_dir: str = "rules", log_level: str = "INFO"):
        """
        Initialize the Rule Engine
        
        Args:
            contracts_dir: Directory containing contract JSON files
            rules_dir: Directory containing rule JSON files
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.contracts_dir = Path(contracts_dir) if contracts_dir else None
        self.rules_dir = Path(rules_dir) if rules_dir else None
        
        # Only initialize contract loader if contracts directory is specified
        if self.contracts_dir and str(self.contracts_dir):
            self.contract_loader = ContractLoader(contracts_dir)
        else:
            self.contract_loader = None
            
        # Only initialize rule loader if rules directory is specified
        if self.rules_dir and str(self.rules_dir):
            self.rule_loader = RuleLoader(rules_dir)
        else:
            self.rule_loader = None
        
        # Cache for loaded contracts to prevent re-loading on every coupon
        self._contracts_cache = None
            
        # Use V2 eligibility checker
        self.eligibility_checker = EligibilityCheckerV2()
        self.computation_engine = ComputationEngine()
        self.addon_processor = AddonRuleProcessor()
        
        # Configure logging
        logger.remove()
        logger.add(
            lambda msg: print(msg, end=""),
            level=log_level,
            format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
        )
        
        if self.contracts_dir:
            logger.info(f"Rule Engine initialized with contracts directory: {self.contracts_dir}")
        else:
            logger.info(f"Rule Engine initialized with rules directory only: {self.rules_dir}")
        
        # Preload contracts once at initialization
        self._load_contracts_cache()
    
    def _load_contracts_cache(self):
        """Load and cache all contracts once for performance"""
        if self._contracts_cache is not None:
            return  # Already cached
            
        try:
            if self.rule_loader:
                self._contracts_cache = self.rule_loader.load_all_rules()
                logger.info(f"Cached {len(self._contracts_cache)} contracts from rules")
            elif self.contract_loader:
                self._contracts_cache = self.contract_loader.load_all_contracts()
                logger.info(f"Cached {len(self._contracts_cache)} contracts")
            else:
                self._contracts_cache = []
        except Exception as e:
            logger.error(f"Error loading contracts cache: {e}")
            self._contracts_cache = []


    def process_single_coupon(self, coupon_data: CouponData) -> ProcessingResult:
        """
        Process a single coupon against all available contracts
        
        Args:
            coupon_data: Validated coupon data
            
        Returns:
            ProcessingResult with all contract analyses
        """
        start_time = datetime.now()
        logger.info(f"Processing coupon: {coupon_data.ticket_number}-{coupon_data.coupon_number}")
        
        try:
            # Step 1: Validate coupon input
            validated_coupon = self._validate_coupon_input(coupon_data)
            
            # Step 2: Use cached contracts (already loaded at initialization)
            # This prevents re-loading contracts for every coupon which causes row duplication
            if self._contracts_cache is None:
                self._load_contracts_cache()
            
            all_contracts = self._contracts_cache
            if not all_contracts:
                logger.warning("No contracts available in cache")
                raise RuleEngineError("No contracts available")
            
            logger.debug(f"Using {len(all_contracts)} cached contracts")
            
            # Initialize output
            result = ProcessingResult(
                coupon_data=validated_coupon,
                airline_eligibility=False
            )
            
            # Step 3: Process all contracts with 3-phase eligibility
            contract_analyses = {}
            contract_count = 0
            eligible_count = 0
            any_sector_eligible = False
            
            for contract in all_contracts:
                try:
                    # STRICT OPTIMIZATION: Check if contract is relevant for this airline
                    # This prevents processing AFKL rules for QR coupons, etc.
                    marketing_airlines = contract.trigger_eligibility_criteria.get('IN', {}).get('Marketing Airline', [])
                    
                    # Also check legacy airline_codes if available
                    legacy_airlines = getattr(contract, 'airline_codes', [])
                    if not legacy_airlines and hasattr(contract, 'iata_codes'):
                         legacy_airlines = contract.iata_codes
                    
                    target_airlines = set(marketing_airlines) | set(legacy_airlines)
                    
                    if target_airlines:
                        # If airline restrictions exist, enforce them
                        # But handle case where coupon might be valid for multiple airlines (rare but possible in data)
                        if validated_coupon.cpn_airline_code not in target_airlines:
                            logger.trace(f"Skipping contract {contract.contract_id}: Airline {validated_coupon.cpn_airline_code} not in {target_airlines}")
                            continue

                    contract_count += 1
                    logger.debug(f"Processing contract {contract_count}: {contract.contract_name}")
                    
                    # Extract formulas and components
                    formulas = self._extract_formulas_and_components(contract)
                    
                    # --- 3-PHASE ELIGIBILITY CHECK ---
                    
                    # Phase 1: Sector Eligibility
                    sector_eligible, sector_reasons = self.eligibility_checker.check_sector_eligibility(
                        validated_coupon, contract
                    )
                    
                    trigger_eligible = False
                    trigger_reasons = []
                    payout_eligible = False
                    payout_reasons = []
                    
                    # Initialize values
                    trigger_value = Decimal('0')
                    payout_value = Decimal('0')
                    
                    if sector_eligible:
                        any_sector_eligible = True
                        
                        # Compute values ONLY if sector is eligible
                        # Apply trigger formula
                        trigger_value = self.computation_engine.compute_trigger(validated_coupon, contract)
                        
                        # Apply payout formula
                        payout_value = self.computation_engine.compute_payout(validated_coupon, contract)
                        
                        # Phase 2: Trigger Eligibility
                        trigger_eligible, trigger_reasons = self.eligibility_checker.check_trigger_eligibility(
                            validated_coupon, contract, sector_eligible
                        )
                        
                        # Phase 3: Payout Eligibility
                        payout_eligible, payout_reasons = self.eligibility_checker.check_payout_eligibility(
                            validated_coupon, contract, sector_eligible
                        )
                    else:
                        # If sector failed, subsequent phases are skipped (already handled in V2 but explicit here for clarity)
                        trigger_reasons = ["Skipped due to sector ineligibility"]
                        payout_reasons = ["Skipped due to sector ineligibility"]

                    # Update global eligible count (requires full trigger eligibility)
                    if trigger_eligible:
                        eligible_count += 1
                    
                    # Create contract analysis
                    contract_analysis = ContractAnalysis(
                        document_name=contract.document_name,
                        document_id=contract.document_id,
                        contract_name=contract.contract_name,
                        contract_id=contract.contract_id,
                        rule_id=contract.rule_id,
                        # New fields for dynamic rule loading
                        ruleset_id=getattr(contract, 'ruleset_id', contract.document_name),
                        source_name=getattr(contract, 'source_name', contract.document_id),
                        currency=getattr(contract, 'currency', 'USD'),  # Currency from contract
                        contract_window_date={
                            "start": contract.start_date,
                            "end": contract.end_date
                        },
                        trigger_formula=formulas['trigger_formula'],
                        trigger_value=trigger_value,
                        payout_formula=formulas['payout_formula'],
                        payout_value=payout_value,
                        
                        # 3-Phase Status
                        sector_eligibility=sector_eligible,
                        trigger_eligibility=trigger_eligible,
                        payout_eligibility=payout_eligible,
                        
                        # Reasons
                        sector_eligibility_reason="; ".join(sector_reasons) if sector_reasons else "Eligible",
                        trigger_eligibility_reason="; ".join(trigger_reasons) if trigger_reasons else "Eligible",
                        payout_eligibility_reason="; ".join(payout_reasons) if payout_reasons else "Eligible",
                        
                        rule_creation_date=contract.creation_date,
                        rule_update_date=contract.update_date
                    )
                    
                    # Process addon rules if any exist (and primary sector check passed)
                    if sector_eligible:
                        addon_result = self.addon_processor.process_addon_rules(
                            validated_coupon, contract, trigger_eligible, payout_eligible, contract_analysis
                        )
                        
                        # Update contract analysis with addon results
                        if addon_result['addon_applied']:
                            contract_analysis = self.addon_processor.update_contract_analysis_with_addon(
                                contract_analysis, addon_result
                            )
                            logger.info(f"Addon rules applied to contract {contract.contract_id}")
                    
                    contract_analyses[f'Contract_{contract_count}'] = contract_analysis
                    
                except Exception as e:
                    logger.error(f"Error processing contract {contract.contract_id}: {str(e)}")
                    
                    # Create a failed contract analysis
                    failed_contract_analysis = ContractAnalysis(
                        document_name=contract.document_name,
                        document_id=contract.document_id,
                        contract_name=contract.contract_name,
                        contract_id=contract.contract_id,
                        rule_id=contract.rule_id,
                        ruleset_id=getattr(contract, 'ruleset_id', contract.document_name),
                        source_name=getattr(contract, 'source_name', contract.document_id),
                        currency=getattr(contract, 'currency', 'USD'),  # Currency from contract
                        contract_window_date={
                            "start": contract.start_date,
                            "end": contract.end_date
                        },
                        trigger_formula="Error in processing",
                        trigger_value=Decimal('0'),
                        payout_formula="Error in processing",
                        payout_value=Decimal('0'),
                        sector_eligibility=False,
                        trigger_eligibility=False,
                        payout_eligibility=False,
                        sector_eligibility_reason=f"Processing error: {str(e)}",
                        trigger_eligibility_reason=f"Processing error: {str(e)}",
                        payout_eligibility_reason=f"Processing error: {str(e)}",
                        rule_creation_date=contract.creation_date,
                        rule_update_date=contract.update_date
                    )
                    
                    contract_analyses[f'Contract_{contract_count}'] = failed_contract_analysis
                    continue
            
            # [NEW] Handle case where no contracts matched the airline
            if not contract_analyses:
                 logger.debug(f"No matching contracts found for airline {validated_coupon.cpn_airline_code}")
                 
                 no_contract_analysis = ContractAnalysis(
                    document_name="N/A",
                    document_id="N/A",
                    contract_name="No Contract Found",
                    contract_id="NO_CONTRACT",
                    rule_id="N/A",
                    ruleset_id="N/A",
                    source_name="N/A",
                    currency=None,  # No currency when no contract
                    contract_window_date={"start": date.min, "end": date.max},
                    trigger_formula="N/A",
                    trigger_value=Decimal('0'),
                    payout_formula="N/A",
                    payout_value=Decimal('0'),
                    
                    # 3-Phase Status - All False
                    sector_eligibility=False,
                    trigger_eligibility=False,
                    payout_eligibility=False,
                    
                    # Reasons
                    sector_eligibility_reason="Contract not available",
                    trigger_eligibility_reason="Contract not available",
                    payout_eligibility_reason="Contract not available",
                    
                    rule_creation_date=date.today(),
                    rule_update_date=date.today()
                 )
                 contract_analyses['No_Contract'] = no_contract_analysis
                 
            # Update result
            result.airline_eligibility = any_sector_eligible  # Best approximation for V1 compatibility
            result.contract_analyses = contract_analyses
            result.total_contracts_processed = contract_count
            result.eligible_contracts = eligible_count
            
            # Calculate processing time
            end_time = datetime.now()
            result.processing_time_ms = int((end_time - start_time).total_seconds() * 1000)
            
            logger.info(f"Processing completed - {contract_count} contracts processed, {eligible_count} eligible")
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing coupon: {str(e)}")
            raise RuleEngineError(f"Failed to process coupon: {str(e)}")
    
    def _validate_coupon_input(self, coupon_data: CouponData) -> CouponData:
        """
        Validate coupon input data
        
        Args:
            coupon_data: Coupon data to validate
            
        Returns:
            Validated coupon data
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            # Basic validation is handled by Pydantic model
            # Additional business logic validation can be added here
            
            if not coupon_data.cpn_airline_code:
                raise ValidationError("Airline code is required")
            
            if coupon_data.cpn_total_revenue <= 0:
                logger.warning(f"Coupon {coupon_data.ticket_number} has non-positive revenue: {coupon_data.cpn_total_revenue}. Processing will continue.")
                # raise ValidationError("Total revenue must be positive") - Valid business case may have 0 revenue
            
            logger.debug("Coupon data validation passed")
            return coupon_data
            
        except Exception as e:
            logger.error(f"Coupon validation failed: {str(e)}")
            raise ValidationError(f"Coupon validation failed: {str(e)}")
    
    def _extract_formulas_and_components(self, contract: ContractData) -> Dict[str, Any]:
        """
        Extract trigger and payout formulas from contract
        
        Args:
            contract: Contract data
            
        Returns:
            Dictionary with formulas and components
        """
        # Use formulas from contract if available, otherwise generate them
        if contract.trigger_formula:
            trigger_formula = contract.trigger_formula
        else:
            trigger_components = ", ".join(contract.trigger_components)
            trigger_formula = f"Sum of {trigger_components} components"
        
        if contract.payout_formula:
            payout_formula = contract.payout_formula
        elif contract.payout_type == "PERCENTAGE":
            payout_components = ", ".join(contract.payout_components)
            payout_formula = f"{contract.payout_percentage}% of {payout_components}"
        else:
            payout_components = ", ".join(contract.payout_components)
            payout_formula = f"Fixed amount based on {payout_components}"
        
        return {
            'trigger_formula': trigger_formula,
            'payout_formula': payout_formula,
            'trigger_components': contract.trigger_components,
            'payout_components': contract.payout_components
        }
    
    def _extract_actual_tier_percentages(self, analysis) -> List[float]:
        """
        Extract actual tier percentages from contract analysis
        
        Args:
            analysis: Contract analysis result
            
        Returns:
            List of tier percentages as decimals (e.g., [0.02, 0.0225, 0.03, 0.035, 0.04, 0.0425])
        """
        try:
            tier_percentages = []
            
            # Get the original contract by loading all contracts and finding the matching one
            if hasattr(analysis, 'contract_id') and hasattr(self, 'rule_loader') and self.rule_loader:
                contract_id = analysis.contract_id
                
                # Load all contracts to find the matching one
                try:
                    all_contracts = self.rule_loader.load_all_rules()
                    original_contract = None
                    
                    for contract in all_contracts:
                        if contract.contract_id == contract_id:
                            original_contract = contract
                            break
                    
                    if original_contract and hasattr(original_contract, 'tiers') and original_contract.tiers:
                        for tier in original_contract.tiers:
                            if isinstance(tier, dict) and 'rows' in tier:
                                for row in tier['rows']:
                                    if isinstance(row, dict):
                                        # Handle both formats: payout.value/unit and Incentive %
                                        payout_value = None
                                        payout_unit = 'PERCENT'
                                        
                                        if 'payout' in row and isinstance(row['payout'], dict):
                                            payout_value = row['payout'].get('value')
                                            payout_unit = row['payout'].get('unit', 'PERCENT')
                                        elif 'Incentive %' in row:
                                            payout_value = row['Incentive %']
                                            payout_unit = 'PERCENT'
                                        elif 'payout_value' in row:
                                            payout_value = row['payout_value']
                                            payout_unit = row.get('payout_unit', 'PERCENT')
                                        
                                        if payout_value is not None and payout_unit == 'PERCENT':
                                            # Convert percentage to decimal (2.0% = 0.02)
                                            tier_percentages.append(float(payout_value) / 100.0)
                
                except Exception as e:
                    logger.warning(f"Failed to load contracts for tier percentage extraction: {str(e)}")
            
            return tier_percentages
            
        except Exception as e:
            logger.error(f"Error extracting tier percentages: {str(e)}")
            return []
    
    def get_contract_summary(self) -> Dict[str, Any]:
        """
        Get summary of all loaded contracts
        
        Returns:
            Dictionary with contract summary information
        """
        # Load all contracts (try rules first, then contracts)
        contracts = []
        if self.rule_loader:
            try:
                contracts = self.rule_loader.load_all_rules()
            except Exception:
                if self.contract_loader:
                    try:
                        contracts = self.contract_loader.load_all_contracts()
                    except Exception:
                        contracts = []
        elif self.contract_loader:
            try:
                contracts = self.contract_loader.load_all_contracts()
            except Exception:
                contracts = []
        
        summary = {
            'total_contracts': len(contracts),
            'contracts_by_airline': {},
            'contracts_by_document': {}
        }
        
        for contract in contracts:
            # Group by airline (extract from contract name or criteria)
            airline = contract.document_name.split('_')[0] if '_' in contract.document_name else 'Unknown'
            if airline not in summary['contracts_by_airline']:
                summary['contracts_by_airline'][airline] = 0
            summary['contracts_by_airline'][airline] += 1
            
            # Group by document
            doc_name = contract.document_name
            if doc_name not in summary['contracts_by_document']:
                summary['contracts_by_document'][doc_name] = 0
            summary['contracts_by_document'][doc_name] += 1
        
        return summary
    
    def generate_json_output(self, processing_result: ProcessingResult, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate JSON output for a processed coupon according to the specified format
        
        Args:
            processing_result: Result from process_single_coupon
            output_file: Optional file path to save JSON output
            
        Returns:
            Dictionary containing the JSON-formatted output
        """
        try:
            coupon = processing_result.coupon_data
            
            # Helper function to safely convert Decimal to float
            def decimal_to_float(value):
                if isinstance(value, Decimal):
                    return float(value)
                return value
            
            # Helper function to format date
            def format_date(date_value):
                if isinstance(date_value, (date, datetime)):
                    return date_value.isoformat()
                elif isinstance(date_value, str):
                    return date_value
                return str(date_value) if date_value else None
            
            # Build coupon_info section
            coupon_info = {
                "ticket_number": coupon.ticket_number,
                "coupon_number": coupon.coupon_number,
                "cpn_airline_code": coupon.cpn_airline_code,
                "cpn_RBD": coupon.cpn_RBD,
                "cabin": coupon.cabin,
                "cpn_total_revenue": decimal_to_float(coupon.cpn_total_revenue),
                "cpn_flown_date": format_date(coupon.cpn_flown_date),
                "cpn_sales_date": format_date(coupon.cpn_sales_date),
                "code_share": coupon.code_share,
                "interline": coupon.interline,
                "ndc": coupon.ndc,
                "cpn_is_international": coupon.cpn_is_international,
                "cpn_revenue_base": decimal_to_float(coupon.cpn_revenue_base),
                "cpn_revenue_yq": decimal_to_float(coupon.cpn_revenue_yq),
                "cpn_revenue_yr": decimal_to_float(coupon.cpn_revenue_yr),
                "cpn_revenue_xt": decimal_to_float(coupon.cpn_revenue_xt),
                "cpn_origin": coupon.cpn_origin,
                "cpn_destination": coupon.cpn_destination,
                "marketing_airline": coupon.marketing_airline,
                "operating_airline": coupon.operating_airline,
                "ticketing_airline": coupon.ticketing_airline
            }
            
            # Build processing_summary section
            processing_summary = {
                "total_contracts_processed": processing_result.total_contracts_processed,
                "eligible_contracts": processing_result.eligible_contracts,
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "rule_engine_version": get_rule_engine_version()
            }
            
            # Build contract_results section
            contract_results = []
            for contract_key, analysis in processing_result.contract_analyses.items():
                # Generate payout calculations with tiers
                payout_calculations = {}
                base_payout = decimal_to_float(analysis.payout_value)
                
                # Extract actual tier percentages from contract
                actual_tier_percentages = self._extract_actual_tier_percentages(analysis)
                
                if actual_tier_percentages:
                    # Use actual tier percentages from contract
                    for i, percentage in enumerate(actual_tier_percentages, 1):
                        tier_amount = base_payout * percentage if base_payout else 0.0
                        payout_calculations[f"tier_{i}"] = {
                            "tier_name": f"Tier {i}",
                            "tier_percentage": percentage * 100,  # Convert to percentage for display
                            "payout_amount": tier_amount
                        }
                else:
                    # Fallback to default percentages if extraction fails
                    tier_percentages = [1.0, 2.0, 3.0, 4.0]
                    for i, percentage in enumerate(tier_percentages, 1):
                        tier_amount = base_payout * percentage if base_payout else 0.0
                        payout_calculations[f"tier_{i}"] = {
                            "tier_name": f"Tier {i}",
                            "tier_percentage": percentage,
                            "payout_amount": tier_amount
                        }
                
                # Extract MTP ID from contract ID or rule ID
                mtp_id = analysis.contract_id
                if '-' in mtp_id and mtp_id.count('-') >= 3:
                    # Extract MTP ID by removing the last segment (e.g., "ET-NG-2025H1-PLB-01" -> "ET-NG-2025H1-PLB")
                    mtp_id = '-'.join(mtp_id.split('-')[:-1])
                
                contract_result = {
                    "contract_id": analysis.contract_id,
                    "contract_name": analysis.contract_name,
                    "contract_mtp_id": mtp_id,
                    "trigger_eligibility": analysis.trigger_eligibility,
                    "payout_eligibility": analysis.payout_eligibility,
                    "trigger_eligibility_reason": analysis.trigger_eligibility_reason,
                    "payout_eligibility_reason": analysis.payout_eligibility_reason,
                    "trigger_formula": analysis.trigger_formula,
                    "payout_formula": analysis.payout_formula,
                    "trigger_value": decimal_to_float(analysis.trigger_value),
                    "payout_calculations": payout_calculations,
                    "eligibility_details": {
                        "airline_eligibility": processing_result.airline_eligibility,
                        "date_eligibility": "Date not in contract window" not in analysis.trigger_eligibility_reason,
                        "geographic_eligibility": "Geographic criteria not met" not in analysis.trigger_eligibility_reason,
                        "booking_eligibility": "Booking criteria not met" not in analysis.trigger_eligibility_reason,
                        "technical_eligibility": "Technical criteria not met" not in analysis.trigger_eligibility_reason,
                        "payout_eligibility": analysis.payout_eligibility
                    }
                }
                contract_results.append(contract_result)
            
            # Build final JSON structure according to user's format
            json_output = {
                "coupon_info": coupon_info,
                "airline_eligibility": processing_result.airline_eligibility,
                "processing_summary": processing_summary,
                "contract_results": contract_results
            }
            
            # Save to file if specified
            if output_file:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(json_output, f, indent=2, ensure_ascii=False)
                
                logger.info(f"JSON output saved to: {output_path}")
            
            return json_output
            
        except Exception as e:
            logger.error(f"Error generating JSON output: {str(e)}")
            raise RuleEngineError(f"Failed to generate JSON output: {str(e)}")
