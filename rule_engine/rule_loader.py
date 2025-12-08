"""
Rule loading and discovery functionality for API-based rule files
"""

import json
import os
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any
from loguru import logger

from .models import ContractData
from .exceptions import ContractError


class RuleLoader:
    """
    Handles loading and parsing of rule JSON files from rules directory
    """
    
    def __init__(self, rules_dir: str = "rules"):
        """
        Initialize rule loader
        
        Args:
            rules_dir: Directory containing rule JSON files
        """
        self.rules_dir = Path(rules_dir)
        self._rules_cache = {}
        self._last_scan = None
        
        if not self.rules_dir.exists():
            logger.warning(f"Rules directory {self.rules_dir} does not exist")
            self.rules_dir.mkdir(parents=True, exist_ok=True)
    
    def load_all_rules(self) -> List[ContractData]:
        """
        Load all contracts from the rules directory with new folder structure:
        rules/airline/year/month/*.json
        
        Returns:
            List of ContractData objects
        """
        contracts = []
        
        try:
            # Scan for JSON files in the new nested structure
            json_files = list(self.rules_dir.glob("**/*.json"))
            logger.info(f"Found {len(json_files)} contract files in nested structure")
            
            for json_file in json_files:
                try:
                    file_contracts = self._load_rule_file(json_file)
                    contracts.extend(file_contracts)
                    logger.debug(f"Loaded {len(file_contracts)} contracts from {json_file.name}")
                except Exception as e:
                    logger.error(f"Failed to load contract from {json_file}: {str(e)}")
                    continue
            
            logger.info(f"Successfully loaded {len(contracts)} total contracts from rules directory")
            return contracts
            
        except Exception as e:
            logger.error(f"Error loading contracts: {str(e)}")
            raise ContractError(f"Failed to load contracts: {str(e)}")
    
    def load_rules_for_airline(self, airline_code: str) -> List[ContractData]:
        """
        Load rules applicable to a specific airline
        
        Args:
            airline_code: Airline code (e.g., "QR", "LH")
            
        Returns:
            List of applicable ContractData objects
        """
        all_rules = self.load_all_rules()
        applicable_rules = []
        
        for rule in all_rules:
            if self._is_rule_applicable_to_airline(rule, airline_code):
                applicable_rules.append(rule)
        
        logger.info(f"Found {len(applicable_rules)} rules for airline {airline_code}")
        return applicable_rules
    
    def load_rules_for_airline_year_month(self, airline_code: str, year: str, month: str) -> List[ContractData]:
        """
        Load rules for a specific airline, year, and month
        
        Args:
            airline_code: Airline code (e.g., "ET", "QR")
            year: Year (e.g., "2025")
            month: Month (e.g., "01", "02")
            
        Returns:
            List of ContractData objects for the specified period
        """
        contracts = []
        
        try:
            # Construct path to specific airline/year/month directory
            target_dir = self.rules_dir / airline_code / year / month
            
            if not target_dir.exists():
                logger.warning(f"Directory {target_dir} does not exist")
                return contracts
            
            # Scan for JSON files in the specific directory
            json_files = list(target_dir.glob("*.json"))
            logger.info(f"Found {len(json_files)} rule files for {airline_code}/{year}/{month}")
            
            for json_file in json_files:
                try:
                    file_contracts = self._load_rule_file(json_file)
                    contracts.extend(file_contracts)
                    logger.debug(f"Loaded {len(file_contracts)} contracts from {json_file.name}")
                except Exception as e:
                    logger.error(f"Failed to load contract from {json_file}: {str(e)}")
                    continue
            
            logger.info(f"Successfully loaded {len(contracts)} contracts for {airline_code}/{year}/{month}")
            return contracts
            
        except Exception as e:
            logger.error(f"Error loading rules for {airline_code}/{year}/{month}: {str(e)}")
            return []
    
    def get_available_rules(self) -> List[Dict[str, Any]]:
        """
        Get metadata about all available rules from the new folder structure
        
        Returns:
            List of rule metadata dictionaries
        """
        rules_metadata = []
        
        try:
            # Scan for JSON files in the new nested structure
            json_files = list(self.rules_dir.glob("**/*.json"))
            
            for json_file in json_files:
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Extract folder structure information
                    relative_path = json_file.relative_to(self.rules_dir)
                    path_parts = relative_path.parts
                    
                    # Extract airline, year, month from path structure
                    airline = path_parts[0] if len(path_parts) > 0 else "Unknown"
                    year = path_parts[1] if len(path_parts) > 1 else "Unknown"
                    month = path_parts[2] if len(path_parts) > 2 else "Unknown"
                    
                    metadata = {
                        "file_name": json_file.name,
                        "file_path": str(relative_path),
                        "airline": airline,
                        "year": year,
                        "month": month,
                        "ruleset_id": data.get("ruleset_id", "Unknown"),
                        "version": data.get("version", "1.0"),
                        "source_name": data.get("metadata", {}).get("source_name", "Unknown"),
                        "start_date": data.get("metadata", {}).get("contract_window", {}).get("start_date", ""),
                        "end_date": data.get("metadata", {}).get("contract_window", {}).get("end_date", ""),
                        "location": data.get("metadata", {}).get("location", ""),
                        "iata_codes": data.get("metadata", {}).get("iata_codes", []),
                        "countries": data.get("metadata", {}).get("countries", []),
                        "rule_count": len(data.get("rules", []))
                    }
                    rules_metadata.append(metadata)
                    
                except Exception as e:
                    logger.error(f"Failed to read metadata from {json_file}: {str(e)}")
                    continue
            
            return rules_metadata
            
        except Exception as e:
            logger.error(f"Error getting rule metadata: {str(e)}")
            return []
    
    def _load_contract_file(self, file_path: Path) -> List[ContractData]:
        """
        Load a single contract file and convert to ContractData objects
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of ContractData objects
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return self._parse_contract_data(data, file_path.name)
            
        except Exception as e:
            logger.error(f"Error loading contract file {file_path}: {str(e)}")
            return []
    
    def _load_rule_file(self, file_path: Path) -> List[ContractData]:
        """
        Load a single rule file and convert to ContractData objects
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            List of ContractData objects
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return self._parse_rule_data(data, file_path.name)
            
        except Exception as e:
            logger.error(f"Error loading rule file {file_path}: {str(e)}")
            return []
    
    def _parse_contract_data(self, data: Dict[str, Any], filename: str) -> List[ContractData]:
        """
        Parse contract JSON data into ContractData objects
        
        Args:
            data: Contract JSON data
            filename: Source filename
            
        Returns:
            List of ContractData objects
        """
        contracts = []
        
        try:
            # Extract document header
            doc_header = data.get('Document_Header', {})
            document_name = doc_header.get('Name', filename)
            start_date = datetime.strptime(doc_header.get('Start Date', '2025-01-01'), '%Y-%m-%d').date()
            end_date = datetime.strptime(doc_header.get('End Date', '2025-12-31'), '%Y-%m-%d').date()
            currency = doc_header.get('Currency', 'NONE')
            location = doc_header.get('Location', 'Unknown')
            iata_codes = doc_header.get('IATA', [])
            countries = doc_header.get('Countries', [])
            
            # Process each MTP (contract)
            contract_index = 1
            for key, mtp_data in data.items():
                if key.startswith('MTP') and isinstance(mtp_data, dict):
                    try:
                        contract = self._parse_single_contract(
                            mtp_data, document_name, start_date, end_date, 
                            currency, location, iata_codes, countries, 
                            filename, contract_index
                        )
                        if contract:
                            contracts.append(contract)
                            contract_index += 1
                    except Exception as e:
                        logger.error(f"Error parsing contract {key} from {filename}: {str(e)}")
                        continue
            
            return contracts
            
        except Exception as e:
            logger.error(f"Error parsing contract data from {filename}: {str(e)}")
            return []
    
    def _parse_single_contract(self, mtp_data: Dict[str, Any], document_name: str, 
                              start_date: date, end_date: date, currency: str, 
                              location: str, iata_codes: List[str], countries: List[str],
                              filename: str, contract_index: int) -> Optional[ContractData]:
        """
        Parse a single MTP contract into ContractData object
        
        Args:
            mtp_data: MTP contract data
            document_name: Document name
            start_date: Contract start date
            end_date: Contract end date
            currency: Currency
            location: Location
            iata_codes: IATA codes
            countries: Countries
            filename: Source filename
            contract_index: Contract index
            
        Returns:
            ContractData object or None if parsing fails
        """
        try:
            # Extract contract details
            contract_name = mtp_data.get('MTP_name', f'Contract {contract_index}')
            contract_type = mtp_data.get('MTP_type', 'Multi Tier TABLES')
            
            # Extract trigger configuration
            trigger_config = mtp_data.get('trigger', {})
            trigger_type = trigger_config.get('type', 'FLOWN')
            trigger_components = trigger_config.get('trigger_components', ['BASE'])
            trigger_eligibility = mtp_data.get('eligibility_criteria', {})
            
            # Fix eligibility criteria structure for QR-AO contracts
            if isinstance(trigger_eligibility, dict) and 'IN' in trigger_eligibility:
                in_criteria = trigger_eligibility['IN']
                if isinstance(in_criteria, list):
                    # Convert list format to dictionary format and add QR airline criteria
                    trigger_eligibility = {
                        'IN': {
                            'Fare Type': in_criteria,
                            'Marketing Airline': ['QR'],
                            'Operating Airline': ['QR'],
                            'Ticketing Airline': ['QR']
                        },
                        'OUT': trigger_eligibility.get('OUT', []),
                        'SILENCE': trigger_eligibility.get('SILENCE', [])
                    }
            
            # Extract payout configuration
            payout_config = mtp_data.get('payout', {})
            payout_type = payout_config.get('type', 'PERCENTAGE')
            payout_components = payout_config.get('payout_components', ['BASE'])
            payout_eligibility = trigger_eligibility  # Use the same eligibility for payout
            
            # Extract tiers
            tiers = mtp_data.get('Tier', [])
            if not tiers:
                tiers = mtp_data.get('tiers', [])
            
            # Calculate payout percentage from tiers
            payout_percentage = self._calculate_payout_percentage_from_contract_tiers(tiers)
            
            # Use original IDs from JSON if available, otherwise generate new ones
            # ruleset_id is at the top level, rule_id is inside each rule object
            original_ruleset_id = mtp_data.get('ruleset_id', '')
            original_rule_id = mtp_data.get('rule_id', '')  # This is the rule_id from the individual rule
            
            if original_ruleset_id and original_rule_id:
                # Use original IDs from JSON
                document_id = original_ruleset_id
                contract_id = original_rule_id
                rule_id = original_rule_id
            else:
                # Fallback to generated IDs if original ones not available
                document_id = f"DOC_{datetime.now().strftime('%Y%m%d')}_{filename.replace('.json', '')}"
                contract_id = f"MTP_{document_id}_{contract_index:03d}"
                rule_id = f"RULE_{contract_id}_001"
            
            contract = ContractData(
                document_name=document_name,
                document_id=document_id,
                contract_name=contract_name,
                contract_id=contract_id,
                rule_id=rule_id,
                # New fields for output mapping
                ruleset_id=filename.replace('.json', ''),
                source_name=document_name,
                start_date=start_date,
                end_date=end_date,
                trigger_type=trigger_type,
                trigger_components=trigger_components,
                trigger_eligibility_criteria=trigger_eligibility,
                payout_type=payout_type,
                payout_components=payout_components,
                payout_percentage=payout_percentage,
                payout_eligibility_criteria=payout_eligibility,
                tiers=tiers,
                creation_date=start_date,
                update_date=end_date,
                iata_codes=iata_codes,
                countries=countries
            )
            
            return contract
            
        except Exception as e:
            logger.error(f"Error parsing single contract: {str(e)}")
            return None
    
    def _calculate_payout_percentage_from_contract_tiers(self, tiers: List[Dict[str, Any]]) -> Optional[Decimal]:
        """
        Calculate average payout percentage from contract tiers
        
        Args:
            tiers: List of tier data
            
        Returns:
            Average payout percentage or None
        """
        try:
            if not tiers:
                return None
            
            total_percentage = Decimal('0')
            count = 0
            
            for tier in tiers:
                rows = tier.get('rows', [])
                for row in rows:
                    payout_value = row.get('payout_value') or row.get('Incentive %')
                    payout_unit = row.get('payout_unit', 'PERCENT')
                    
                    if payout_unit == 'PERCENT' and payout_value is not None:
                        total_percentage += Decimal(str(payout_value))
                        count += 1
            
            if count > 0:
                avg_percentage = total_percentage / count
                print(f"✅ Calculated contract payout percentage: {avg_percentage}% (from {count} tier rows)")
                return avg_percentage
            
            return None
            
        except Exception as e:
            print(f"❌ Error calculating contract payout percentage: {e}")
            return None

    def _parse_rule_data(self, data: Dict[str, Any], filename: str) -> List[ContractData]:
        """
        Parse rule JSON data into ContractData objects
        
        Args:
            data: Parsed JSON data
            filename: Source filename
            
        Returns:
            List of ContractData objects
        """
        contracts = []
        
        try:
            # Extract metadata and ruleset_id from top level
            metadata = data.get('metadata', {})
            metadata['ruleset_id'] = data.get('ruleset_id', filename.replace('.json', ''))
            contract_window = metadata.get('contract_window', {})
            
            # Parse dates
            start_date = self._parse_date(contract_window.get('start_date', '2025-01-01'))
            end_date = self._parse_date(contract_window.get('end_date', '2025-12-31'))
            
            # Process each rule in the ruleset
            rules = data.get('rules', [])
            
            for i, rule in enumerate(rules, 1):
                try:
                    contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, i)
                    if contract:
                        contracts.append(contract)
                except Exception as e:
                    logger.error(f"Error parsing rule {i} in {filename}: {str(e)}")
                    continue
            
            return contracts
            
        except Exception as e:
            logger.error(f"Error parsing rule data from {filename}: {str(e)}")
            return []
    
    def _parse_single_rule(self, rule: Dict[str, Any], metadata: Dict[str, Any], 
                          start_date: date, end_date: date, filename: str, rule_index: int) -> Optional[ContractData]:
        """
        Parse a single rule into ContractData object
        
        Args:
            rule: Single rule data
            metadata: Rule metadata
            start_date: Contract start date
            end_date: Contract end date
            filename: Source filename
            rule_index: Index of rule in file
            
        Returns:
            ContractData object or None if parsing fails
        """
        try:
            # Extract rule details with proper unique identifiers
            rule_id = rule.get('rule_id', metadata.get('ruleset_id', f'RULE_{rule_index}'))
            rule_name = rule.get('name', metadata.get('source_name', f'Rule {rule_index}'))
            rule_type = rule.get('type', 'Multi Tier TABLES')
            
            # Extract what_if section
            what_if = rule.get('what_if', {})
            trigger_config = what_if.get('trigger', {})
            payout_config = what_if.get('payout', {})
            
            # Extract trigger configuration
            trigger_components = trigger_config.get('components', ['BASE'])
            trigger_formula = trigger_config.get('formula', 'Sum of BASE components')
            
            # Determine trigger type based on variant flags
            variant_flags = rule.get('variant_flags', {})
            if variant_flags.get('Sales', False):
                trigger_type = 'SALES'
            elif variant_flags.get('NFR', False):
                trigger_type = 'FLOWN'
            else:
                # Fallback to trigger config type
                trigger_type = trigger_config.get('type', 'FLOWN')
            
            # Extract payout configuration
            payout_components = payout_config.get('components', ['BASE'])
            payout_type = payout_config.get('type', 'PERCENTAGE')
            payout_formula = payout_config.get('formula', f'{payout_type} of {payout_components}')
            
            # Extract tiers first
            tiers = self._extract_tiers_from_rule(rule)
            
            # If there are tiers, use tiered payout instead of percentage
            if tiers and any(tier.get('rows') for tier in tiers):
                payout_type = 'TIERED'
                payout_percentage = None
            else:
                # Calculate payout percentage from tiers
                payout_percentage = self._calculate_payout_percentage_from_rule(rule)
            
            # Extract eligibility criteria
            trigger_eligibility = rule.get('where_trigger', {})
            payout_eligibility = rule.get('where_payout', {})
            
            # Extract addon rule cases
            addon_rule_cases = rule.get('addon_rule_cases', [])
            
            # Use original IDs from JSON if available, otherwise generate new ones
            original_ruleset_id = metadata.get('ruleset_id', '')
            original_rule_id = rule_id  # This is the rule_id from JSON
            
            if original_ruleset_id and original_rule_id:
                # Use original IDs from JSON
                document_id = original_ruleset_id
                contract_id = original_rule_id
                # Keep rule_id as the original from JSON
            else:
                # Fallback to generated IDs if original ones not available
                document_id = f"DOC_{datetime.now().strftime('%Y%m%d')}_{filename.replace('.json', '')}"
                contract_id = f"MTP_{document_id}_{rule_index}"
                rule_id = f"RULE_{contract_id}_001"
            
            contract = ContractData(
                document_name=metadata.get('source_name', filename),
                document_id=document_id,
                contract_name=rule_name,
                contract_id=contract_id,
                rule_id=rule_id,
                # New fields for output mapping
                ruleset_id=metadata.get('ruleset_id', filename.replace('.json', '')),
                source_name=metadata.get('source_name', filename),
                start_date=start_date,
                end_date=end_date,
                trigger_type=trigger_type,
                trigger_components=trigger_components,
                trigger_formula=trigger_formula,
                trigger_eligibility_criteria=trigger_eligibility,
                payout_type=payout_type,
                payout_components=payout_components,
                payout_formula=payout_formula,
                payout_percentage=payout_percentage,
                payout_eligibility_criteria=payout_eligibility,
                tiers=tiers,
                creation_date=datetime.now().date(),
                update_date=datetime.now().date(),
                iata_codes=metadata.get('iata_codes', []),
                countries=metadata.get('countries', []),
                addon_rule_cases=addon_rule_cases
            )
            
            return contract
            
        except Exception as e:
            logger.error(f"Error parsing single rule: {str(e)}")
            return None
    
    def _parse_date(self, date_str: str) -> date:
        """
        Parse date string to date object
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            date object
        """
        if isinstance(date_str, str):
            # Try common date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f%z']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
        
        # Default to current date if parsing fails
        return datetime.now().date()
    
    def _calculate_payout_percentage_from_rule(self, rule: Dict[str, Any]) -> Optional[Decimal]:
        """
        Calculate average payout percentage from rule tiers - Zen: Simple and Direct
        
        Args:
            rule: Rule data
            
        Returns:
            Average payout percentage or None
        """
        try:
            # Look for tiers in the rule structure (correct structure)
            tiers = rule.get('tiers', [])
            if not tiers:
                return None
            
            total_percentage = Decimal('0')
            count = 0
            
            for tier in tiers:
                rows = tier.get('rows', [])
                for row in rows:
                    payout_info = row.get('payout', {})
                    payout_value = payout_info.get('value', 0)
                    payout_unit = payout_info.get('unit', '')
                    
                    if payout_unit == 'PERCENT':
                        total_percentage += Decimal(str(payout_value))
                        count += 1
            
            if count > 0:
                avg_percentage = total_percentage / count
                print(f"✅ Calculated payout percentage: {avg_percentage}% (from {count} tier rows)")
                return avg_percentage
            
            return None
            
        except Exception as e:
            print(f"❌ Error calculating payout percentage: {e}")
            return None
    
    def _extract_tiers_from_rule(self, rule: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract tiers from rule data
        
        Args:
            rule: Rule data
            
        Returns:
            List of tier data
        """
        return rule.get('tiers', [])
    
    def _is_rule_applicable_to_airline(self, rule: ContractData, airline_code: str) -> bool:
        """
        Check if a rule is applicable to a specific airline
        
        Args:
            rule: ContractData object
            airline_code: Airline code to check
            
        Returns:
            True if applicable, False otherwise
        """
        # Check trigger eligibility criteria
        trigger_criteria = rule.trigger_eligibility_criteria
        if isinstance(trigger_criteria, dict):
            # Check IN criteria for marketing airline
            in_criteria = trigger_criteria.get('IN', {})
            marketing_airlines = in_criteria.get('Marketing Airline', [])
            if marketing_airlines and airline_code not in marketing_airlines:
                return False
            
            # Check IN criteria for operating airline
            operating_airlines = in_criteria.get('Operating Airline', [])
            if operating_airlines and airline_code not in operating_airlines:
                return False
            
            # Check IN criteria for ticketing airline
            ticketing_airlines = in_criteria.get('Ticketing Airline', [])
            if ticketing_airlines and airline_code not in ticketing_airlines:
                return False
        
        return True
