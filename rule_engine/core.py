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
from .eligibility_checker import EligibilityChecker
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
        self.eligibility_checker = EligibilityChecker()
        self.computation_engine = ComputationEngine()
        self.addon_processor = AddonRuleProcessor()
        
        # Configure logging
        logger.remove()
        # logger.add(
        #     "rule_engine.log",
        #     level=log_level,
        #     format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}",
        #     rotation="10 MB",
        #     retention="30 days"
        # )
        logger.add(
            lambda msg: print(msg, end=""),
            level=log_level,
            format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
        )
        
        if self.contracts_dir:
            logger.info(f"Rule Engine initialized with contracts directory: {self.contracts_dir}")
        else:
            logger.info(f"Rule Engine initialized with rules directory only: {self.rules_dir}")
    
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
            
            # Step 2: Find all available contracts (try rules first, then contracts)
            all_contracts = []
            if self.rule_loader:
                try:
                    all_contracts = self.rule_loader.load_all_rules()
                    logger.info(f"Found {len(all_contracts)} total rules")
                except Exception as e:
                    logger.warning(f"Failed to load rules: {e}, trying contracts")
                    if self.contract_loader:
                        try:
                            all_contracts = self.contract_loader.load_all_contracts()
                            logger.info(f"Found {len(all_contracts)} total contracts")
                        except Exception as e2:
                            logger.error(f"Failed to load contracts: {e2}")
                            raise
            elif self.contract_loader:
                try:
                    all_contracts = self.contract_loader.load_all_contracts()
                    logger.info(f"Found {len(all_contracts)} total contracts")
                except Exception as e:
                    logger.error(f"Failed to load contracts: {e}")
                    raise
            else:
                logger.error("No contract or rule loader available")
                raise RuleEngineError("No contract or rule loader available")
            
            # Initialize output
            result = ProcessingResult(
                coupon_data=validated_coupon,
                airline_eligibility=False
            )
            
            # Step 3: Filter contracts by airline eligibility first
            eligible_contracts = []
            for contract in all_contracts:
                if self.eligibility_checker.check_airline_eligibility(validated_coupon, contract):
                    eligible_contracts.append(contract)
                    logger.debug(f"Contract {contract.contract_id} eligible for airline {validated_coupon.cpn_airline_code}")
                else:
                    logger.debug(f"Contract {contract.contract_id} not eligible for airline {validated_coupon.cpn_airline_code}")
            
            if not eligible_contracts:
                logger.info(f"No contracts eligible for airline {validated_coupon.cpn_airline_code}")
                return ProcessingResult(
                    coupon_data=validated_coupon,
                    airline_eligibility=False,
                    total_contracts_processed=0,
                    eligible_contracts=0,
                    contract_analyses={}
                )
            
            logger.info(f"Processing {len(eligible_contracts)} eligible contracts for airline {validated_coupon.cpn_airline_code}")
            
            # Step 4: Process each eligible contract
            contract_analyses = {}
            contract_count = 0
            eligible_count = 0
            airline_passed_check = True  # Airline passed eligibility check
            
            for contract in eligible_contracts:
                try:
                    
                    contract_count += 1
                    logger.info(f"Processing contract {contract_count}: {contract.contract_name}")
                    
                    # Extract formulas and components
                    formulas = self._extract_formulas_and_components(contract)
                    
                    # Apply trigger formula
                    trigger_value = self.computation_engine.compute_trigger(validated_coupon, contract)
                    
                    # Apply payout formula
                    payout_value = self.computation_engine.compute_payout(validated_coupon, contract)
                    
                    # Check all eligibility criteria using the comprehensive method
                    eligibility_results = self.eligibility_checker.check_all_eligibility_criteria(validated_coupon, contract)
                    
                    # Extract individual eligibility results
                    date_eligible = eligibility_results['date_eligibility']
                    geo_eligible = eligibility_results['geographic_eligibility']
                    booking_eligible = eligibility_results['booking_eligibility']
                    technical_eligible = eligibility_results['technical_eligibility']
                    payout_eligible = eligibility_results['payout_eligibility']
                    
                    # Determine final eligibility
                    trigger_eligible = eligibility_results['trigger_eligibility']
                    
                    # Build eligibility reasons
                    trigger_reasons = []
                    payout_reasons = []
                    
                    if not date_eligible:
                        trigger_reasons.append("Date not in contract window")
                        payout_reasons.append("Date not in contract window")
                    if not geo_eligible:
                        trigger_reasons.append("Geographic criteria not met")
                        payout_reasons.append("Geographic criteria not met")
                    if not booking_eligible:
                        trigger_reasons.append("Booking criteria not met")
                        payout_reasons.append("Booking criteria not met")
                    if not technical_eligible:
                        trigger_reasons.append("Technical criteria not met")
                        payout_reasons.append("Technical criteria not met")
                    if not payout_eligible:
                        payout_reasons.append("Payout criteria not met")
                    
                    if trigger_eligible:
                        trigger_reasons.append("All trigger criteria met")
                        eligible_count += 1
                    
                    if payout_eligible:
                        payout_reasons.append("All payout criteria met")
                    
                    # Create initial contract analysis
                    contract_analysis = ContractAnalysis(
                        document_name=contract.document_name,
                        document_id=contract.document_id,
                        contract_name=contract.contract_name,
                        contract_id=contract.contract_id,
                        rule_id=contract.rule_id,
                        # New fields for dynamic rule loading
                        ruleset_id=getattr(contract, 'ruleset_id', contract.document_name),
                        source_name=getattr(contract, 'source_name', contract.document_id),
                        contract_window_date={
                            "start": contract.start_date,
                            "end": contract.end_date
                        },
                        trigger_formula=formulas['trigger_formula'],
                        trigger_value=trigger_value,
                        payout_formula=formulas['payout_formula'],
                        payout_value=payout_value,
                        trigger_eligibility=trigger_eligible,
                        payout_eligibility=payout_eligible,
                        trigger_eligibility_reason="; ".join(trigger_reasons),
                        payout_eligibility_reason="; ".join(payout_reasons),
                        rule_creation_date=contract.creation_date,
                        rule_update_date=contract.update_date
                    )
                    
                    # Process addon rules if any exist
                    addon_result = self.addon_processor.process_addon_rules(
                        validated_coupon, contract, trigger_eligible, payout_eligible, contract_analysis
                    )
                    
                    # Update contract analysis with addon results
                    if addon_result['addon_applied']:
                        contract_analysis = self.addon_processor.update_contract_analysis_with_addon(
                            contract_analysis, addon_result
                        )
                        logger.info(f"Addon rules applied to contract {contract.contract_id} - Trigger: {contract_analysis.trigger_eligibility}, Payout: {contract_analysis.payout_eligibility}")
                    
                    # Update final eligibility counts based on addon results
                    if contract_analysis.trigger_eligibility and not trigger_eligible:
                        eligible_count += 1
                        logger.info(f"Contract {contract.contract_id} became eligible due to addon rules")
                    
                    contract_analyses[f'Contract_{contract_count}'] = contract_analysis
                    
                    logger.info(f"Contract {contract_count} processed - Trigger: {contract_analysis.trigger_eligibility}, Payout: {contract_analysis.payout_eligibility}")
                    
                except Exception as e:
                    logger.error(f"Error processing contract {contract.contract_id}: {str(e)}")
                    
                    # Create a failed contract analysis instead of skipping
                    failed_contract_analysis = ContractAnalysis(
                        document_name=contract.document_name,
                        document_id=contract.document_id,
                        contract_name=contract.contract_name,
                        contract_id=contract.contract_id,
                        rule_id=contract.rule_id,
                        ruleset_id=getattr(contract, 'ruleset_id', contract.document_name),
                        source_name=getattr(contract, 'source_name', contract.document_id),
                        contract_window_date={
                            "start": contract.start_date,
                            "end": contract.end_date
                        },
                        trigger_formula="Error in processing",
                        trigger_value=Decimal('0'),
                        payout_formula="Error in processing",
                        payout_value=Decimal('0'),
                        trigger_eligibility=False,
                        payout_eligibility=False,
                        trigger_eligibility_reason=f"Processing error: {str(e)}",
                        payout_eligibility_reason=f"Processing error: {str(e)}",
                        rule_creation_date=contract.creation_date,
                        rule_update_date=contract.update_date
                    )
                    
                    contract_analyses[f'Contract_{contract_count}'] = failed_contract_analysis
                    logger.info(f"Contract {contract_count} failed - Error: {str(e)}")
                    continue
            
            # Update result - airline is eligible if it passed airline eligibility check for any contract
            result.airline_eligibility = airline_passed_check
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
                raise ValidationError("Total revenue must be positive")
            
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
