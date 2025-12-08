#!/usr/bin/env python3
"""
Integrated PLB Rule Engine - Single function for Databricks integration
"""

import pandas as pd
import json
import os
import sys
from pathlib import Path
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from dateutil import parser
from rule_engine import RuleEngine
from rule_engine.models import CouponData
from rule_engine.rule_loader import RuleLoader


def get_rule_engine_version() -> str:
    """Get the current rule engine version"""
    # Always return the current library version
    # This ensures consistency regardless of installed package versions
    return "1.2.0"


def get_utc_timestamp() -> str:
    """Get current UTC timestamp in ISO format"""
    return datetime.now(timezone.utc).isoformat()


def parse_date_with_default_year(date_str: str, default_year: int = 2025) -> date:
    """
    Parse date string with flexible format handling and default year
    
    Args:
        date_str: Date string to parse (e.g., "16-Apr", "15-Apr-25", "2025-04-16")
        default_year: Default year to use if not specified (default: 2025)
        
    Returns:
        date object
    """
    if not date_str or pd.isna(date_str) or str(date_str).strip() == "":
        return date(default_year, 1, 1)
    
    date_str = str(date_str).strip()
    
    try:
        # Try parsing with dateutil first (handles most formats)
        parsed_date = parser.parse(date_str, default=datetime(default_year, 1, 1))
        return parsed_date.date()
    except (ValueError, TypeError):
        pass
    
    # Try specific formats
    formats_to_try = [
        "%d-%b-%y",      # 15-Apr-25
        "%d-%b",         # 15-Apr (will use default year)
        "%Y-%m-%d",      # 2025-04-15
        "%d/%m/%Y",      # 15/04/2025
        "%d/%m/%y",      # 15/04/25
        "%d-%m-%Y",      # 15-04-2025
        "%d-%m-%y",      # 15-04-25
    ]
    
    for fmt in formats_to_try:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            # If the format doesn't include year, use default year
            if fmt == "%d-%b":
                parsed_date = parsed_date.replace(year=default_year)
            return parsed_date.date()
        except ValueError:
            continue
    
    # If all parsing fails, return default date
    print(f"Warning: Could not parse date '{date_str}', using default date {default_year}-01-01")
    return date(default_year, 1, 1)


class PLBRuleEngine:
    """
    Integrated PLB Rule Engine for Databricks pipeline integration
    """
    
    def __init__(self, contracts_dir: str = "contracts", output_dir: str = "output"):
        """
        Initialize the PLB Rule Engine
        
        Args:
            contracts_dir: Directory containing contract JSON files
            output_dir: Directory for output files
        """
        self.contracts_dir = contracts_dir
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize rule loader and engine - only use rules directory
        self.rule_loader = RuleLoader("rules")  # Load from rules directory
        self.engine = RuleEngine(contracts_dir="", rules_dir="rules", log_level="INFO")
        self.engine.contract_loader = self.rule_loader
        
        # Cache rules for performance - load once, use many times (Zen principle)
        self._cached_rules = None
        self._load_rules_cache()
    
    def process_csv_file(self, input_file: str, output_file: str = None, output_format: str = "json") -> Dict[str, Any]:
        """
        Process CSV file with coupon data and generate one comprehensive JSON file for the entire batch
        
        Args:
            input_file: Path to input CSV file
            output_file: Optional output file path (defaults to output/result.json)
            output_format: Output format - "json" or "csv" (defaults to json)
            
        Returns:
            Dictionary with processing results and statistics
        """
        print(f"Processing CSV: {input_file}")
        
        # Set default output file path
        if output_file is None:
            output_file = "output/result.json"
        
        output_path = Path(output_file)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load CSV data
        try:
            df = pd.read_csv(input_file)
            print(f"Loaded {len(df)} coupons from CSV")
        except Exception as e:
            return {"error": f"Failed to load CSV: {str(e)}"}
        
        # Process each coupon and collect results
        batch_results = []
        processed_coupons = []
        errors = 0
        
        for index, row in df.iterrows():
            try:
                # Convert row to CouponData
                coupon = self._row_to_coupon_data(row)
                
                    # Note: Airline filtering is now handled in the core engine
                    # No need for early filtering here as the core engine will filter contracts by airline
                
                # Process coupon
                result = self.engine.process_single_coupon(coupon)
                
                if output_format.lower() == "json":
                    # Generate JSON output for this coupon
                    json_output = self.engine.generate_json_output(result)
                    batch_results.append(json_output)
                    print(f"Processed coupon {index + 1}: {coupon.ticket_number}_{coupon.coupon_number}")
                
                processed_coupons.append({
                    "coupon_id": f"{coupon.ticket_number}_{coupon.coupon_number}",
                    "airline": coupon.cpn_airline_code,
                    "airline_eligible": result.airline_eligibility,
                    "contracts_processed": result.total_contracts_processed,
                    "contracts_eligible": result.eligible_contracts
                })
                
            except Exception as e:
                errors += 1
                print(f"❌ Error processing coupon {index + 1}: {e}")
        
        # Generate batch summary
        total_processed = len(processed_coupons)
        eligible_coupons = sum(1 for c in processed_coupons if c["airline_eligible"])
        
        if output_format.lower() == "json":
            # Create one comprehensive JSON file for the entire batch
            batch_json = {
                "batch_processing_summary": {
                    "input_file": str(input_file),
                    "output_file": str(output_path),
                    "output_format": output_format,
                    "total_coupons_in_file": len(df),
                    "total_coupons_processed": total_processed,
                    "total_coupons_eligible": eligible_coupons,
                    "total_errors": errors,
                    "processing_timestamp": get_utc_timestamp(),
                    "rule_engine_version": get_rule_engine_version(),
                    "processing_environment": {
                        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                        "platform": os.name,
                        "hostname": os.environ.get('HOSTNAME', 'unknown')
                    }
                },
                "coupons": batch_results
            }
            
            # Save the comprehensive JSON file to the specified path
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(batch_json, f, indent=2, ensure_ascii=False)
            
            print(f"\n📊 Processing Summary:")
            print(f"   Total coupons in file: {len(df)}")
            print(f"   Coupons processed: {total_processed}")
            print(f"   Coupons eligible: {eligible_coupons}")
            print(f"   Errors: {errors}")
            print(f"   JSON file saved to: {output_path}")
            
            return {
                "success": True,
                "output_file": str(output_path),
                "total_coupons_processed": total_processed,
                "total_coupons_eligible": eligible_coupons,
                "errors": errors,
                "batch_processing_summary": batch_json["batch_processing_summary"]
            }
        
        else:
            # Fallback for CSV format (if needed)
            return {
                "success": True,
                "output_file": str(output_path),
                "total_coupons_processed": total_processed,
                "total_coupons_eligible": eligible_coupons,
                "errors": errors,
                "message": "CSV format processing not implemented in this version"
            }
    
    def get_available_contracts(self) -> List[Dict[str, Any]]:
        """
        Get list of available rules (contracts are now loaded from rules directory)
        
        Returns:
            List of rule information
        """
        try:
            rules = self.rule_loader.load_all_rules()
            rule_list = []
            
            for rule in rules:
                rule_info = {
                    "contract_id": rule.contract_id,
                    "document_name": rule.document_name,
                    "contract_name": rule.contract_name,
                    "airline_codes": self._extract_airline_codes(rule),
                    "start_date": rule.start_date.isoformat(),
                    "end_date": rule.end_date.isoformat(),
                    "payout_type": rule.payout_type,
                    "trigger_components": rule.trigger_components,
                    "payout_components": rule.payout_components
                }
                rule_list.append(rule_info)
            
            return rule_list
            
        except Exception as e:
            return [{"error": f"Failed to load rules: {str(e)}"}]
    
    def get_available_rules(self) -> List[Dict[str, Any]]:
        """
        Get list of available rules
        
        Returns:
            List of rule information
        """
        try:
            rules_metadata = self.rule_loader.get_available_rules()
            return rules_metadata
            
        except Exception as e:
            return [{"error": f"Failed to load rules: {str(e)}"}]
    
    def fetch_rules(self) -> Dict[str, Any]:
        """
        Fetch all rules from the rules directory (API-friendly function)
        
        Returns:
            Dictionary with rules data and metadata
        """
        try:
            # Get rule metadata
            rules_metadata = self.rule_loader.get_available_rules()
            
            # Load all rules as contracts
            contracts = self.rule_loader.load_all_rules()
            
            # Convert contracts to API-friendly format
            rules_data = []
            for contract in contracts:
                rule_data = {
                    "rule_id": contract.rule_id,
                    "contract_id": contract.contract_id,
                    "contract_name": contract.contract_name,
                    "document_name": contract.document_name,
                    "start_date": contract.start_date.isoformat(),
                    "end_date": contract.end_date.isoformat(),
                    "trigger_type": contract.trigger_type,
                    "trigger_components": contract.trigger_components,
                    "payout_type": contract.payout_type,
                    "payout_components": contract.payout_components,
                    "payout_percentage": float(contract.payout_percentage) if contract.payout_percentage else None,
                    "iata_codes": contract.iata_codes,
                    "countries": contract.countries,
                    "creation_date": contract.creation_date.isoformat(),
                    "update_date": contract.update_date.isoformat()
                }
                rules_data.append(rule_data)
            
            return {
                "success": True,
                "rules_count": len(rules_data),
                "metadata": rules_metadata,
                "rules": rules_data,
                "fetched_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to fetch rules: {str(e)}",
                "rules_count": 0,
                "metadata": [],
                "rules": [],
                "fetched_at": datetime.now().isoformat()
            }
    
    def _row_to_coupon_data(self, row: pd.Series) -> CouponData:
        """Convert pandas row to CouponData object with robust error handling"""
        coupon_dict = row.to_dict()
        
        # Enhanced null and data type handling
        for key, value in coupon_dict.items():
            try:
                if pd.isna(value) or value is None or (isinstance(value, str) and value.strip() == ""):
                    if key in ['cabin', 'airline_name', 'marketing_airline', 'ticketing_airline', 'operating_airline']:
                        coupon_dict[key] = "Unknown"
                    elif key in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue', 'flight_number', 'iata']:
                        coupon_dict[key] = 0.0
                    elif key in ['cpn_sales_date', 'cpn_flown_date']:
                        coupon_dict[key] = parse_date_with_default_year(value, 2025)
                    elif key in ['code_share', 'interline', 'ndc', 'cpn_is_international']:
                        coupon_dict[key] = False
                    else:
                        coupon_dict[key] = ""
                elif isinstance(value, (int, float)) and pd.isna(value):
                    if key in ['cabin', 'airline_name', 'marketing_airline', 'ticketing_airline', 'operating_airline']:
                        coupon_dict[key] = "Unknown"
                    elif key in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue', 'flight_number', 'iata']:
                        coupon_dict[key] = 0.0
                    else:
                        coupon_dict[key] = ""
                elif isinstance(value, str):
                    # Clean string values and handle dates
                    if key in ['cpn_sales_date', 'cpn_flown_date']:
                        coupon_dict[key] = parse_date_with_default_year(value.strip(), 2025)
                    else:
                        coupon_dict[key] = value.strip()
            except Exception as e:
                # Fallback for any conversion errors
                if key in ['cabin', 'airline_name', 'marketing_airline', 'ticketing_airline', 'operating_airline']:
                    coupon_dict[key] = "Unknown"
                elif key in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue', 'flight_number', 'iata']:
                    coupon_dict[key] = 0.0
                else:
                    coupon_dict[key] = ""
        
        # Map column names to match CouponData model
        column_mapping = {
            'Marketing Airline': 'marketing_airline',
            'Ticketing Airline': 'ticketing_airline',
            'Corporate Code': 'corporate_code',
            'Operating Airline': 'operating_airline',
            'City Codes': 'city_codes',
            'Route': 'route',
            'Code Share': 'code_share',
            'Interline': 'interline',
            'Tour Codes': 'tour_codes',
            'Fare Type': 'fare_type',
            'DomIntl': 'cpn_is_international'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in coupon_dict:
                coupon_dict[new_name] = coupon_dict.pop(old_name)
        
        # Handle missing airline columns by using cpn_airline_code as fallback
        if 'marketing_airline' not in coupon_dict or pd.isna(coupon_dict.get('marketing_airline')) or coupon_dict.get('marketing_airline') == '':
            coupon_dict['marketing_airline'] = coupon_dict.get('cpn_airline_code', '')
        if 'ticketing_airline' not in coupon_dict or pd.isna(coupon_dict.get('ticketing_airline')) or coupon_dict.get('ticketing_airline') == '':
            coupon_dict['ticketing_airline'] = coupon_dict.get('cpn_airline_code', '')
        
        # Ensure all required fields are present and properly typed
        required_fields = {
            'source_system': str,
            'pcc': str,
            'ticket_number': str,
            'coupon_number': str,
            'cpn_airline_code': str,
            'cpn_fare_basis': str,
            'cpn_RBD': str,
            'iata': str,
            'cabin': str,
            'cpn_origin': str,
            'cpn_destination': str,
            'flight_number': str,
            'airline_name': str,
            'cpn_sales_date': str,
            'cpn_flown_date': str,
            'cpn_revenue_base': float,
            'cpn_revenue_yq': float,
            'cpn_revenue_yr': float,
            'cpn_revenue_xt': float,
            'cpn_total_revenue': float
        }
        
        for field, field_type in required_fields.items():
            if field in coupon_dict:
                if coupon_dict[field] is not None and not (isinstance(coupon_dict[field], float) and pd.isna(coupon_dict[field])):
                    try:
                        coupon_dict[field] = field_type(coupon_dict[field])
                    except (ValueError, TypeError):
                        # Use default values for conversion errors
                        if field in ['cpn_sales_date', 'cpn_flown_date']:
                            coupon_dict[field] = parse_date_with_default_year(coupon_dict.get(field, ""), 2025)
                        elif field in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                            coupon_dict[field] = 0.0
                        elif field in ['cabin', 'airline_name']:
                            coupon_dict[field] = "Unknown"
                        else:
                            coupon_dict[field] = ""
                else:
                    # Handle None or NaN values
                    if field in ['cpn_sales_date', 'cpn_flown_date']:
                        coupon_dict[field] = parse_date_with_default_year(coupon_dict.get(field, ""), 2025)
                    elif field in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                        coupon_dict[field] = 0.0
                    elif field in ['cabin', 'airline_name']:
                        coupon_dict[field] = "Unknown"
                    else:
                        coupon_dict[field] = ""
            else:
                # Field not present in data
                if field in ['cpn_sales_date', 'cpn_flown_date']:
                    coupon_dict[field] = parse_date_with_default_year("", 2025)
                elif field in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                    coupon_dict[field] = 0.0
                elif field in ['cabin', 'airline_name']:
                    coupon_dict[field] = "Unknown"
                else:
                    coupon_dict[field] = ""
        
        return CouponData(**coupon_dict)
    
    def _create_result_row(self, coupon: CouponData, result, original_row: pd.Series = None) -> Dict[str, Any]:
        """Create result row in the correct format from working document"""
        
        # Start with all original input data (dynamic mapping)
        result_row = {}
        
        # If original_row is provided, include all input columns dynamically
        if original_row is not None:
            for col_name, col_value in original_row.items():
                # Handle special cases for data type conversion
                if col_name in ['cpn_sales_date', 'cpn_flown_date']:
                    result_row[col_name] = str(col_value) if col_value is not None else ""
                elif col_name in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                    try:
                        result_row[col_name] = float(col_value) if col_value is not None and not pd.isna(col_value) else 0.0
                    except (ValueError, TypeError):
                        result_row[col_name] = 0.0
                elif col_name in ['ticket_number', 'flight_number']:
                    result_row[col_name] = str(col_value) if col_value is not None else ""
                elif col_name in ['cpn_is_international', 'tkt_is_international', 'code_share', 'interline']:
                    # Handle boolean fields
                    if col_value is None or pd.isna(col_value):
                        result_row[col_name] = False
                    elif isinstance(col_value, str):
                        result_row[col_name] = col_value.lower() in ['true', '1', 'yes', 'y']
                    else:
                        result_row[col_name] = bool(col_value)
                else:
                    # Default handling for other columns
                    result_row[col_name] = col_value if col_value is not None and not pd.isna(col_value) else ""
        
        # Add rule engine specific fields
        result_row.update({
            "Airline Eligibility": result.airline_eligibility,
            "Total Contracts Processed": result.total_contracts_processed,
            "Eligible Contracts": result.eligible_contracts,
            "contract_mtp_id": ""  # Will be populated with contract-specific MTP IDs
        })
        
        # Add contract analyses (up to 10 contracts for better coverage)
        contract_mtp_ids = []  # Collect MTP IDs for the main contract_mtp_id field
        
        for i, (contract_id, analysis) in enumerate(result.contract_analyses.items(), 1):
            if i <= 10:  # Increased limit to 10 contracts for CSV
                contract_prefix = f"Contract_{i}"
                # Map according to requirements:
                # Contract_x_Document_Name -> ruleset_id 
                # Contract_x_Document_ID -> source_name
                # Contract_x_Contract_Name -> rule_id
                # Contract_x_Contract_ID -> ruleset_id+rule_id
                ruleset_id = getattr(analysis, 'ruleset_id', analysis.document_name or 'Unknown')
                rule_id = getattr(analysis, 'rule_id', analysis.contract_id or 'Unknown')
                source_name = getattr(analysis, 'source_name', analysis.document_id or 'Unknown')
                
                # Create MTP identifier for this contract
                mtp_id = f"{ruleset_id}_{rule_id}"
                contract_mtp_ids.append(mtp_id)
                
                # Use unique contract ID to avoid duplicates - use actual contract_id for unique naming
                unique_contract_id = f"{ruleset_id}_{rule_id}_{i}"
                
                # Zen: Clean, simple output - use contract_id for unique rule names
                # Combine contract_id with index to ensure uniqueness
                clean_rule_name = f"{rule_id}_{i}".replace('QR-', '').replace('-PLB', '').replace('_', ' ')
                # Calculate tier computed values using actual formula with multipliers
                # Pass the original coupon data for proper revenue calculation
                tier_values = self._calculate_tier_values(analysis, i, coupon)
                
                # Get actual formulas from the original contract
                actual_trigger_formula, actual_payout_formula = self._extract_actual_formulas_from_analysis(analysis)
                
                # Debug: Print extracted formulas for verification
                print(f"Contract_{i} formulas - Trigger: {actual_trigger_formula}, Payout: {actual_payout_formula}")
                
                result_row.update({
                    f"{contract_prefix}_Rule": clean_rule_name,
                    f"{contract_prefix}_Period": f"{analysis.contract_window_date.start.strftime('%b-%y') if hasattr(analysis.contract_window_date, 'start') else 'Unknown'} to {analysis.contract_window_date.end.strftime('%b-%y') if hasattr(analysis.contract_window_date, 'end') else 'Unknown'}",
                    f"{contract_prefix}_Trigger_Formula": actual_trigger_formula,
                    f"{contract_prefix}_Trigger_Value": f"{float(analysis.trigger_value):.2f}" if analysis.trigger_value else "0.00",
                    f"{contract_prefix}_Trigger_Eligible": analysis.trigger_eligibility,
                    f"{contract_prefix}_Payout_Formula": actual_payout_formula, 
                    f"{contract_prefix}_Payout_Eligible": analysis.payout_eligibility,
                    f"{contract_prefix}_Trigger_Reason": analysis.trigger_eligibility_reason if analysis.trigger_eligibility_reason is not None else "Eligible",
                    f"{contract_prefix}_Payout_Reason": analysis.payout_eligibility_reason if analysis.payout_eligibility_reason is not None else "Eligible"
                })
                
                # Add tier computed values
                result_row.update(tier_values)
        
        # Update the main contract_mtp_id field with all MTP IDs
        if contract_mtp_ids:
            result_row["contract_mtp_id"] = "|".join(contract_mtp_ids)
        
        return result_row
    
    def _calculate_tier_values(self, analysis, contract_index: int, coupon: CouponData) -> Dict[str, str]:
        """
        Calculate computed payout values for each tier using actual tier percentages from rules
        Zen: Use actual tier percentages from rule JSON files (0.5%, 1%, 2%, 3.5%, 5%, etc.)
        """
        try:
            tier_values = {}
            contract_prefix = f"Contract_{contract_index}"
            
            # Get base revenue for calculation
            base_revenue = float(coupon.cpn_revenue_base) if coupon.cpn_revenue_base else 0.0
            
            # Extract actual tier percentages from the rule
            tier_percentages = self._extract_tier_percentages_from_analysis(analysis)
            
            # Debug: Print extracted tier percentages for verification
            if tier_percentages:
                print(f"Extracted tier percentages for Contract_{contract_index}: {tier_percentages}")
                print(f"   Formula: Tier1={tier_percentages.get(1, 0)*100:.1f}%*base, Tier2={tier_percentages.get(2, 0)*100:.1f}%*base, etc.")
            else:
                print(f"Using fallback percentages for Contract_{contract_index}")
                # Fallback to default percentages if extraction fails
                # Use 4 tiers to match ET rule structure (convert to decimal format)
                tier_percentages = {1: 0.01, 2: 0.02, 3: 0.03, 4: 0.04}
            
            # Calculate tier values using actual tier percentages from rules
            # Formula: Base Revenue * Tier Percentage (already in decimal format)
            # Use the actual number of tiers from the rule, not hardcoded 5
            max_tiers = max(tier_percentages.keys()) if tier_percentages else 0
            for tier_num in range(1, max_tiers + 1):
                tier_percentage = tier_percentages.get(tier_num, 0.0)
                tier_value = base_revenue * tier_percentage
                tier_values[f"{contract_prefix}_Tier{tier_num}"] = f"{tier_value:.4f}"
            
            return tier_values
            
        except Exception as e:
            print(f"Error calculating tier values: {e}")
            return {}
    
    def _extract_tier_percentages_from_analysis(self, analysis) -> Dict[int, float]:
        """
        Extract actual tier percentages from rule analysis
        Zen: Get actual tier percentages from rule JSON files (0.5%, 1%, 2%, 3.5%, 5%, etc.)
        """
        try:
            tier_percentages = {}
            
            # The analysis object doesn't have direct access to contract tiers
            # We need to get the original contract from the cached rules
            if hasattr(analysis, 'contract_id'):
                contract_id = analysis.contract_id
                
                # Find the original contract in cached rules
                original_contract = None
                if self._cached_rules:
                    for contract in self._cached_rules:
                        if contract.contract_id == contract_id:
                            original_contract = contract
                            break
                
                if original_contract and hasattr(original_contract, 'tiers') and original_contract.tiers:
                    tier_index = 1
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
                                        # Convert percentage to decimal (1% = 0.01)
                                        tier_percentages[tier_index] = float(payout_value) / 100.0
                                        tier_index += 1
            
            return tier_percentages
            
        except Exception as e:
            print(f"Error extracting tier percentages: {e}")
            return {}
    
    def _extract_actual_formulas_from_analysis(self, analysis) -> tuple[str, str]:
        """
        Extract actual formulas from rule analysis
        Zen: Get actual formulas from rule JSON files instead of hardcoded ones
        """
        try:
            # Get the original contract from cached rules
            if hasattr(analysis, 'contract_id') and self._cached_rules:
                contract_id = analysis.contract_id
                
                for contract in self._cached_rules:
                    if contract.contract_id == contract_id:
                        # Extract actual formulas from the rule JSON structure
                        trigger_formula = self._build_trigger_formula(contract)
                        payout_formula = self._build_payout_formula(contract)
                        return trigger_formula, payout_formula
            
            # Fallback to analysis formulas if contract not found
            trigger_formula = analysis.trigger_formula if analysis.trigger_formula is not None else "N/A"
            payout_formula = analysis.payout_formula if analysis.payout_formula is not None else "N/A"
            return trigger_formula, payout_formula
            
        except Exception as e:
            print(f"Error extracting actual formulas: {e}")
            return "N/A", "N/A"
    
    def _build_trigger_formula(self, contract) -> str:
        """Build trigger formula from contract data"""
        try:
            components = getattr(contract, 'trigger_components', ['BASE'])
            trigger_type = getattr(contract, 'trigger_type', 'FLOWN')
            
            if 'NONE' in components:
                if trigger_type == 'FLOWN':
                    return f"Sum of total revenue components (Flown Revenue)"
                else:
                    return f"Sum of total revenue components (Sales Revenue)"
            else:
                if trigger_type == 'FLOWN':
                    return f"Sum of {', '.join(components)} components (Flown Revenue)"
                else:
                    return f"Sum of {', '.join(components)} components (Sales Revenue)"
        except:
            return "N/A"
    
    def _build_payout_formula(self, contract) -> str:
        """Build payout formula from contract data"""
        try:
            components = getattr(contract, 'payout_components', ['BASE'])
            payout_type = getattr(contract, 'payout_type', 'PERCENTAGE')
            
            if payout_type == 'PERCENTAGE':
                # Handle NONE components
                if 'NONE' in components:
                    return f"Fixed amount based on total revenue"
                
                # Get actual tier percentages from the contract
                if hasattr(contract, 'tiers') and contract.tiers:
                    tier_formulas = []
                    for tier in contract.tiers:
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
                                        tier_formulas.append(f"Tier: {payout_value}% of {', '.join(components)}")
                    
                    if tier_formulas:
                        return f"Tier-based: {'; '.join(tier_formulas)}"
                
                # Fallback to payout percentage if available
                payout_percentage = getattr(contract, 'payout_percentage', None)
                if payout_percentage:
                    return f"{payout_percentage}% of {', '.join(components)}"
                else:
                    return f"Percentage of {', '.join(components)} (tier-based)"
            else:
                if 'NONE' in components:
                    return f"Fixed amount based on total revenue"
                else:
                    return f"Fixed amount based on {', '.join(components)}"
        except:
            return "N/A"
    
    def _create_error_row(self, row: pd.Series, error_msg: str) -> Dict[str, Any]:
        """Create error row for failed processing"""
        # Start with all original input data (dynamic mapping)
        result_row = {}
        
        # Include all input columns dynamically
        for col_name, col_value in row.items():
            # Handle special cases for data type conversion
            if col_name in ['cpn_sales_date', 'cpn_flown_date']:
                result_row[col_name] = str(col_value) if col_value is not None else ""
            elif col_name in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                try:
                    result_row[col_name] = float(col_value) if col_value is not None and not pd.isna(col_value) else 0.0
                except (ValueError, TypeError):
                    result_row[col_name] = 0.0
            elif col_name in ['ticket_number', 'flight_number']:
                result_row[col_name] = str(col_value) if col_value is not None else ""
            elif col_name in ['cpn_is_international', 'tkt_is_international', 'code_share', 'interline']:
                # Handle boolean fields
                if col_value is None or pd.isna(col_value):
                    result_row[col_name] = False
                elif isinstance(col_value, str):
                    result_row[col_name] = col_value.lower() in ['true', '1', 'yes', 'y']
                else:
                    result_row[col_name] = bool(col_value)
            else:
                # Default handling for other columns
                result_row[col_name] = col_value if col_value is not None and not pd.isna(col_value) else ""
        
        # Add rule engine fields - all false/zero for error rows
        result_row.update({
            "error": error_msg,
            "Airline Eligibility": False,
            "Total Contracts Processed": 0,
            "Eligible Contracts": 0,
            "contract_mtp_id": ""  # Empty for error rows
        })
        
        return result_row
    
    def _extract_airline_codes(self, contract) -> List[str]:
        """Extract airline codes from contract"""
        try:
            trigger_criteria = contract.trigger_eligibility_criteria
            in_criteria = trigger_criteria.get('IN', {})
            marketing_airlines = in_criteria.get('Marketing Airline', [])
            return marketing_airlines
        except:
            return []
    
    def _load_rules_cache(self):
        """Load and cache all rules for performance - Zen: Do it once, do it right"""
        try:
            self._cached_rules = self.rule_loader.load_all_rules()
            print(f"Cached {len(self._cached_rules)} rules for optimal performance")
        except Exception as e:
            print(f"Error caching rules: {e}")
            self._cached_rules = []
    
    def _is_airline_eligible_for_any_rule(self, coupon: CouponData) -> bool:
        """
        Check if coupon's airline is eligible for any available rule
        Zen: Use cached rules for performance, simple airline matching
        """
        try:
            # Use cached rules for performance
            if not self._cached_rules:
                return True  # Process if cache failed
            
            # Check if coupon matches any rule's airline criteria
            for rule in self._cached_rules:
                trigger_criteria = rule.trigger_eligibility_criteria.get('IN', {})
                
                # Check marketing airline
                marketing_airlines = trigger_criteria.get('Marketing Airline', [])
                if marketing_airlines:  # Only check if list is not empty
                    # Use cpn_airline_code as fallback if marketing_airline is "Unknown"
                    airline_to_check = coupon.marketing_airline if coupon.marketing_airline != "Unknown" else coupon.cpn_airline_code
                    if airline_to_check in marketing_airlines:
                        return True
                
                # Check operating airline  
                operating_airlines = trigger_criteria.get('Operating Airline', [])
                if operating_airlines:  # Only check if list is not empty
                    airline_to_check = coupon.operating_airline if coupon.operating_airline != "Unknown" else coupon.cpn_airline_code
                    if airline_to_check in operating_airlines:
                        return True
                        
                # Check ticketing airline
                ticketing_airlines = trigger_criteria.get('Ticketing Airline', [])
                if ticketing_airlines:  # Only check if list is not empty
                    airline_to_check = coupon.ticketing_airline if coupon.ticketing_airline != "Unknown" else coupon.cpn_airline_code
                    if airline_to_check in ticketing_airlines:
                        return True
            
            return False
            
        except Exception as e:
            print(f"Error in airline eligibility check: {e}")
            return True  # Process if unsure

    def _process_single_coupon_with_cached_rules(self, coupon: CouponData):
        """
        Process a single coupon using cached rules instead of loading them repeatedly
        Zen: Use cached rules for optimal performance
        """
        from rule_engine.models import ProcessingResult, ContractAnalysis
        from datetime import datetime
        
        start_time = datetime.now()
        
        try:
            # Use cached rules instead of loading them
            all_contracts = self._cached_rules or []
            
            # Initialize output
            result = ProcessingResult(
                coupon_data=coupon,
                airline_eligibility=False
            )
            
            # Process each contract
            contract_analyses = {}
            contract_count = 0
            eligible_count = 0
            
            for contract in all_contracts:
                try:
                    # Check airline eligibility first
                    if not self.engine.eligibility_checker.check_airline_eligibility(coupon, contract):
                        continue
                    
                    contract_count += 1
                    
                    # Process the contract
                    analysis = self.engine._process_contract(coupon, contract)
                    
                    if analysis.trigger_eligibility or analysis.payout_eligibility:
                        eligible_count += 1
                    
                    contract_analyses[contract.contract_id] = analysis
                    
                except Exception as e:
                    print(f"Error processing contract {getattr(contract, 'contract_id', 'unknown')}: {e}")
                    continue
            
            # Set airline eligibility based on eligible contracts (not just processed)
            result.airline_eligibility = eligible_count > 0
            result.contract_analyses = contract_analyses
            result.total_contracts_processed = contract_count
            result.eligible_contracts = eligible_count
            result.processing_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return result
            
        except Exception as e:
            print(f"Error in _process_single_coupon_with_cached_rules: {e}")
            # Fallback to original method if something goes wrong
            return self.engine.process_single_coupon(coupon)
    
    def _create_ineligible_result_row(self, coupon: CouponData, reason: str, original_row: pd.Series = None) -> dict:
        """Create result row for ineligible coupon without processing contracts"""
        # Start with all original input data (dynamic mapping)
        result_row = {}
        
        # If original_row is provided, include all input columns dynamically
        if original_row is not None:
            for col_name, col_value in original_row.items():
                # Handle special cases for data type conversion
                if col_name in ['cpn_sales_date', 'cpn_flown_date']:
                    result_row[col_name] = str(col_value) if col_value is not None else ""
                elif col_name in ['cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 'cpn_revenue_xt', 'cpn_total_revenue']:
                    try:
                        result_row[col_name] = float(col_value) if col_value is not None and not pd.isna(col_value) else 0.0
                    except (ValueError, TypeError):
                        result_row[col_name] = 0.0
                elif col_name in ['ticket_number', 'flight_number']:
                    result_row[col_name] = str(col_value) if col_value is not None else ""
                elif col_name in ['cpn_is_international', 'tkt_is_international', 'code_share', 'interline']:
                    # Handle boolean fields
                    if col_value is None or pd.isna(col_value):
                        result_row[col_name] = False
                    elif isinstance(col_value, str):
                        result_row[col_name] = col_value.lower() in ['true', '1', 'yes', 'y']
                    else:
                        result_row[col_name] = bool(col_value)
                else:
                    # Default handling for other columns
                    result_row[col_name] = col_value if col_value is not None and not pd.isna(col_value) else ""
        
        # Add rule engine fields - all false/zero for ineligible
        result_row.update({
            "error": reason,
            "Airline Eligibility": False,
            "Total Contracts Processed": 0,
            "Eligible Contracts": 0,
            "contract_mtp_id": ""  # Empty for ineligible coupons
        })
        
        return result_row
    
    def _calculate_statistics(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate processing statistics"""
        return {
            "total_coupons": len(results_df),
            "airline_eligible": results_df['Airline Eligibility'].sum(),
            "total_contracts_processed": results_df['Total Contracts Processed'].sum(),
            "eligible_contracts": results_df['Eligible Contracts'].sum(),
            "airline_breakdown": results_df['cpn_airline_code'].value_counts().to_dict()
        }


    def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process a pandas DataFrame directly and return a processed DataFrame with row explosion
        
        Args:
            df: Input DataFrame with coupon data
            
        Returns:
            Processed DataFrame with one row per applicable rule (or one row if no rules apply)
        """
        print(f"Processing DataFrame with {len(df)} rows")
        
        results = []
        
        def get_ist_time():
            utc_now = datetime.now(timezone.utc)
            try:
                import pytz
                ist_tz = pytz.timezone('Asia/Kolkata')
                return utc_now.astimezone(ist_tz).isoformat()
            except ImportError:
                # Fallback: simple offset addition layout if needed
                # For now just return ISO format with timezone info (UTC)
                # But to satisfy requirement of IST roughly:
                from datetime import timedelta
                ist_now = utc_now + timedelta(hours=5, minutes=30)
                return ist_now.replace(tzinfo=None).isoformat() + "+05:30"

        for index, row in df.iterrows():
            try:
                # Convert row to CouponData
                coupon = self._row_to_coupon_data(row)
                
                # Process coupon
                result = self.engine.process_single_coupon(coupon)
                
                # Base row data from input
                # Convert input row to dict to preserve all original columns
                base_row_data = row.to_dict()
                
                # Common processing metadata
                base_row_data['processed_time'] = get_ist_time()
                base_row_data['Processing_error'] = ""
                base_row_data['Sector_Airline_Eligibility'] = result.airline_eligibility
                
                # Row Explosion Logic
                if result.contract_analyses:
                    for contract_key, analysis in result.contract_analyses.items():
                        # Create a new row for each contract
                        output_row = base_row_data.copy()
                        
                        # Populate rule-specific fields
                        output_row.update({
                            'Contract_Document_Name': analysis.document_name,
                            'Contract_Document_ID': analysis.document_id,
                            'Contract_Name': analysis.contract_name,
                            'Contract_ID': analysis.contract_id,
                            'Contract_Document_Start_Date': analysis.contract_window_date.start,
                            'Contract_Document_Window_End': analysis.contract_window_date.end,
                            'Contract_Rule_Creation_Date': analysis.rule_creation_date,
                            'Contract_Rule_Update_Date': analysis.rule_update_date,
                            'Contract_Status': "Active",
                            
                            'Trigger_Formula': analysis.trigger_formula,
                            'Trigger_Value': float(analysis.trigger_value) if analysis.trigger_value is not None else 0.0,
                            'Trigger_Eligibility': analysis.trigger_eligibility,
                            'Trigger_Eligibility_Reason': analysis.trigger_eligibility_reason,
                            
                            'Payout_Eligibility': analysis.payout_eligibility,
                            'Payout_Formula': analysis.payout_formula,
                            'Payout_Eligibility_Reason': analysis.payout_eligibility_reason,
                        })
                        
                        # Calculate and populate Tier values
                        tier_data = self._calculate_tier_values_v2(analysis, coupon)
                        output_row.update(tier_data)
                        
                        results.append(output_row)
                else:
                    # No rules applicable - return single row with empty rule fields
                    output_row = base_row_data.copy()
                    
                    output_row.update({
                        'Contract_Document_Name': None,
                        'Contract_Document_ID': None,
                        'Contract_Name': None,
                        'Contract_ID': None,
                        'Contract_Document_Start_Date': None,
                        'Contract_Document_Window_End': None,
                        'Contract_Rule_Creation_Date': None,
                        'Contract_Rule_Update_Date': None,
                        'Contract_Status': None,
                        'Trigger_Formula': None,
                        'Trigger_Value': None,
                        'Trigger_Eligibility': None,
                        'Trigger_Eligibility_Reason': None,
                        'Payout_Eligibility': None,
                        'Payout_Formula': None,
                        'Payout_Eligibility_Reason': None,
                    })
                    
                    results.append(output_row)
                    
            except Exception as e:
                # Error handling - return row with error message
                error_row = row.to_dict()
                error_row['processed_time'] = get_ist_time()
                error_row['Processing_error'] = str(e)
                error_row['Sector_Airline_Eligibility'] = False
                results.append(error_row)
                print(f"Error processing row {index}: {e}")

        # Create DataFrame from results
        output_df = pd.DataFrame(results)
        
        # Ensure all requested output columns are present even if empty
        expected_columns = [
            'Sector_Airline_Eligibility', 'processed_time', 'Contract_Document_Name', 
            'Contract_Document_ID', 'Contract_Name', 'Contract_ID', 
            'Contract_Document_Start_Date', 'Contract_Document_Window_End', 
            'Contract_Rule_Creation_Date', 'Contract_Rule_Update_Date', 'Contract_Status',
            'Trigger_Formula', 'Trigger_Value', 'Trigger_Eligibility', 'Trigger_Eligibility_Reason',
            'Payout_Eligibility', 'Payout_Formula', 'Payout_Eligibility_Reason',
            'Processing_error'
        ]
        
        # Add tier columns to expected list (1 to 10)
        for i in range(1, 11):
            expected_columns.append(f'Tier{i}_percent')
            expected_columns.append(f'tier{i}_payout')
        
        # Reorder columns: Input columns first, then expected output columns
        if not output_df.empty:
            existing_cols = [c for c in output_df.columns if c not in expected_columns]
            final_order = existing_cols + expected_columns
            # Filter to only columns that actually exist or add missing ones
            for col in expected_columns:
                if col not in output_df.columns:
                    output_df[col] = None
            
            # Zen: Robust generalized flow to preserve data types from input
            # If input column is object type, ensure output is string-filled (not Null/Void)
            # This handles any column that comes empty or null but should be string
            common_cols = output_df.columns.intersection(df.columns)
            for col in common_cols:
                if pd.api.types.is_object_dtype(df[col]):
                    # If input was object (string-like), ensure output is not None
                    if col in output_df.columns:
                        output_df[col] = output_df[col].fillna("").astype(str)
            
            output_df = output_df[final_order]
        else:
            return pd.DataFrame(columns=list(df.columns) + expected_columns)
            
        return output_df

    def _calculate_tier_values_v2(self, analysis, coupon: CouponData) -> Dict[str, Any]:
        """
        Calculate tier values for the new output format (TierX_percent, tierX_payout)
        """
        tier_data = {}
        
        # Initialize 10 tiers with None
        for i in range(1, 11):
            tier_data[f'Tier{i}_percent'] = None
            tier_data[f'tier{i}_payout'] = None
            
        try:
            base_revenue = float(coupon.cpn_revenue_base) if coupon.cpn_revenue_base else 0.0
            
            # Extract actual tier percentages
            tier_percentages = self._extract_tier_percentages_from_analysis(analysis)
            
            # If no tiers found, fallback
            if not tier_percentages:
                 tier_percentages = {1: 0.01, 2: 0.02, 3: 0.03, 4: 0.04}
            
            # Populate data
            for tier_num, percentage in tier_percentages.items():
                if tier_num > 10:
                    continue
                    
                payout = base_revenue * percentage
                
                tier_data[f'Tier{tier_num}_percent'] = percentage * 100 # stored as percent value (e.g. 5.0 for 5%)
                tier_data[f'tier{tier_num}_payout'] = payout
                
        except Exception as e:
            print(f"Error calculating tier values v2: {e}")
            
        return tier_data


def main():
    """CLI interface for the integrated rule engine"""
    engine = PLBRuleEngine(contracts_dir="contracts")
    
    while True:
        print("\n" + "="*60)
        print("PLB RULE ENGINE - INTEGRATED (BATCH JSON OUTPUT)")
        print("="*60)
        print("1. Process CSV file (Batch JSON Output)")
        print("2. Show available contracts")
        print("3. Show available rules")
        print("4. Fetch contracts from contracts directory")
        print("5. Exit")
        print("-"*60)
        
        choice = input("Select option (1-5): ").strip()
        
        if choice == "1":
            input_file = input("Enter CSV file path (Default: input/ET_data_fixed.csv): ").strip()
            if not input_file:
                input_file = "input/ET_data_fixed.csv"
            
            if not Path(input_file).exists():
                print(f"❌ File not found: {input_file}")
                continue
            
            output_file = input("Enter output file path (Default: output/result.json): ").strip()
            if not output_file:
                output_file = "output/result.json"
            
            print(f"\n🔄 Processing {input_file}...")
            print(f"📄 Output file: {output_file}")
            result = engine.process_csv_file(input_file, output_file=output_file, output_format="json")
            
            if result.get("success"):
                print("Batch processing completed successfully!")
                print(f"📄 Output file: {result['output_file']}")
                print(f"📋 Statistics:")
                print(f"   - Total coupons processed: {result['total_coupons_processed']}")
                print(f"   - Total coupons eligible: {result['total_coupons_eligible']}")
                print(f"   - Errors: {result['errors']}")
                print(f"   - All coupon data saved in one comprehensive JSON file")
            else:
                print(f"❌ Error: {result.get('error', 'Unknown error')}")
        
        elif choice == "2":
            print("\n📋 Available Contracts:")
            contracts = engine.get_available_contracts()
            if contracts and not contracts[0].get('error'):
                for i, contract in enumerate(contracts, 1):
                    print(f"{i}. {contract['contract_name']}")
                    print(f"   ID: {contract['contract_id']}")
                    print(f"   Airlines: {', '.join(contract['airline_codes'])}")
                    print(f"   Period: {contract['start_date']} to {contract['end_date']}")
                    print(f"   Type: {contract['payout_type']}")
                    print()
            else:
                print("❌ No contracts available or error loading contracts")
        
        elif choice == "3":
            print("\n📋 Available Rules:")
            rules = engine.get_available_rules()
            if rules and not rules[0].get('error'):
                for i, rule in enumerate(rules, 1):
                    print(f"{i}. {rule['contract_name']}")
                    print(f"   ID: {rule['contract_id']}")
                    print(f"   Airlines: {', '.join(rule['airline_codes'])}")
                    print(f"   Period: {rule['start_date']} to {rule['end_date']}")
                    print(f"   Type: {rule['payout_type']}")
                    print()
            else:
                print("❌ No rules available or error loading rules")
        
        elif choice == "4":
            print("\n🔄 Fetching contracts from contracts directory...")
            result = engine.fetch_rules()
            
            if result.get("success"):
                print("Contracts fetched successfully!")
                print(f"📊 Contracts count: {result['rules_count']}")
                print(f"📁 Fetched at: {result['fetched_at']}")
                print("\n📋 Contract Metadata:")
                for i, metadata in enumerate(result['metadata'], 1):
                    print(f"{i}. {metadata['source_name']}")
                    print(f"   Ruleset ID: {metadata['ruleset_id']}")
                    print(f"   Version: {metadata['version']}")
                    print(f"   Period: {metadata['start_date']} to {metadata['end_date']}")
                    print(f"   Location: {metadata['location']}")
                    print(f"   Contract Count: {metadata['rule_count']}")
                    print()
            else:
                print(f"❌ Error fetching contracts: {result.get('error', 'Unknown error')}")
        
        elif choice == "5":
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid option. Please select 1-5.")


if __name__ == "__main__":
    main()
