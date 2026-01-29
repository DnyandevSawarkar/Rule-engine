"""
Rule loading and discovery functionality for API-based rule files
"""

import json
import os
from datetime import datetime, date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Any
from loguru import logger

try:
    from dateutil.relativedelta import relativedelta
except ImportError:
    # Fallback if dateutil not available - will use timedelta for months
    relativedelta = None

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
            List of ContractData objects (deduplicated by contract_id)
        """
        # Always rescan the filesystem so that deleted/added rules
        # are reflected immediately. Rebuild the in‑memory cache
        # on every call instead of returning stale entries.
        self._rules_cache = {}
        contracts: List[ContractData] = []
        seen_contract_ids = set()
        
        try:
            # Scan for JSON files in the new nested structure
            json_files = list(self.rules_dir.glob("**/*.json"))
            logger.info(f"Found {len(json_files)} contract files in nested structure")
            
            for json_file in json_files:
                try:
                    file_contracts = self._load_rule_file(json_file)
                    for contract in file_contracts:
                        # Deduplicate by contract_id to prevent row explosion
                        if contract.contract_id not in seen_contract_ids:
                            contracts.append(contract)
                            seen_contract_ids.add(contract.contract_id)
                            self._rules_cache[contract.contract_id] = contract
                        else:
                            logger.debug(f"Skipping duplicate contract: {contract.contract_id}")
                    logger.debug(f"Loaded {len(file_contracts)} contracts from {json_file.name}")
                except Exception as e:
                    logger.error(f"Failed to load contract from {json_file}: {str(e)}")
                    continue
            
            logger.info(f"Successfully loaded {len(contracts)} unique contracts from rules directory")
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
                countries=countries,
                airline_codes=[str(code).strip().upper() for code in (trigger_eligibility.get('IN', {}).get('Marketing Airline', []) or []) if code]
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
                        try:
                            total_percentage += Decimal(str(payout_value))
                            count += 1
                        except Exception as e:
                            # Gracefully handle bad numeric values in legacy contract tiers
                            # instead of bubbling up Decimal(ConversionSyntax)
                            print(f"❌ Skipping invalid payout_value '{payout_value}' in contract tiers: {e}")
            
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
            
            # Extract currency from metadata (can be string or array)
            currency_val = metadata.get('currency', 'USD')
            if isinstance(currency_val, list):
                currency = currency_val[0] if currency_val else 'USD'
            else:
                currency = currency_val if currency_val else 'USD'
            metadata['currency'] = currency
            
            # Parse dates
            start_date = self._parse_date(contract_window.get('start_date', '2025-01-01'))
            end_date = self._parse_date(contract_window.get('end_date', '2025-12-31'))
            
            # Process each rule in the ruleset
            rules = data.get('rules', [])
            
            for i, rule in enumerate(rules, 1):
                try:
                    # Check for roll-back-to-zero with auto-generation
                    reset_config = self._parse_reset_config(rule)
                    
                    if reset_config and reset_config.get('generate_mtps', False):
                        # Generate multiple MTPs based on reset_config
                        mtp_contracts = self._generate_mtps_for_rollback(
                            rule, metadata, start_date, end_date, filename, reset_config
                        )
                        contracts.extend(mtp_contracts)
                    else:
                        # Single MTP (existing behavior or generate_mtps: false)
                        contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, i)
                        if contract:
                            # Set evaluation_basis if it's roll-back-to-zero but not auto-generating
                            if reset_config:
                                contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                                contract.reset_config = reset_config
                            contracts.append(contract)
                except Exception as e:
                    logger.error(f"Error parsing rule {i} in {filename}: {str(e)}")
                    continue
            
            return contracts
            
        except Exception as e:
            logger.error(f"Error parsing rule data from {filename}: {str(e)}")
            return []

    def _normalize_and_enforce_airline_criteria(
        self,
        criteria: Dict[str, Any],
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Normalize rule JSON keys to the internal contract schema and
        enforce airline-level restriction using metadata.airline_codes.

        This fixes cases where:
        - JSON uses keys like 'marketing_carrier' instead of 'Marketing Airline'
        - Rules do not explicitly restrict by airline, causing QR/ET/4Z to match EK rules, etc.
        """
        if not isinstance(criteria, dict):
            return criteria or {}

        in_crit = criteria.get("IN", {}) or {}
        out_crit = criteria.get("OUT", {}) or {}

        # 1) Normalize carrier keys (marketing_carrier -> Marketing Airline, etc.)
        carrier_key_map = {
            "marketing_carrier": "Marketing Airline",
            "operating_carrier": "Operating Airline",
            "ticketing_carrier": "Ticketing Airline",
        }

        for src_key, dst_key in carrier_key_map.items():
            if src_key in in_crit and dst_key not in in_crit:
                in_crit[dst_key] = in_crit.pop(src_key)
            if src_key in out_crit and dst_key not in out_crit:
                out_crit[dst_key] = out_crit.pop(src_key)

        # 2) Enforce airline restriction from metadata.airline_codes if not already present
        airline_codes = metadata.get("airline_codes") or metadata.get("airline_codes".lower()) or []
        if airline_codes and isinstance(airline_codes, list):
            has_airline_filter = any(
                key in in_crit
                for key in ("Marketing Airline", "Operating Airline", "Ticketing Airline")
            )
            if not has_airline_filter:
                # Default: restrict by marketing + operating airline
                in_crit.setdefault("Marketing Airline", airline_codes)
                in_crit.setdefault("Operating Airline", airline_codes)

        # Rebuild criteria dict
        criteria["IN"] = in_crit
        criteria["OUT"] = out_crit
        criteria.setdefault("SILENT", criteria.get("SILENT", {}))
        return criteria
    
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
            
            # Check for rule-specific active_window (for period-specific dates)
            active_window = what_if.get('active_window', {})
            if active_window:
                # Use rule-specific dates from active_window
                period_start = self._parse_date(active_window.get('from', start_date))
                period_end = self._parse_date(active_window.get('to', end_date))
            else:
                # Fallback to metadata contract_window dates
                period_start = start_date
                period_end = end_date
            
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
            trigger_eligibility = rule.get('where_trigger', {}) or {}
            payout_eligibility = rule.get('where_payout', {}) or {}

            # Normalize airline-related keys and enforce airline restriction
            trigger_eligibility = self._normalize_and_enforce_airline_criteria(
                trigger_eligibility, metadata
            )
            payout_eligibility = self._normalize_and_enforce_airline_criteria(
                payout_eligibility, metadata
            )
            
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
                currency=metadata.get('currency', 'USD'),  # Currency from metadata
                start_date=period_start,  # Use period-specific dates
                end_date=period_end,      # Use period-specific dates
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
                airline_codes=metadata.get('airline_codes', []),
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
                        try:
                            total_percentage += Decimal(str(payout_value))
                            count += 1
                        except Exception as e:
                            # Gracefully handle bad numeric values in rule tiers
                            print(f"❌ Skipping invalid payout_value '{payout_value}' in rule tiers: {e}")
            
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
    
    def _parse_reset_config(self, rule: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse and validate reset_config from rule JSON
        
        Args:
            rule: Rule data dictionary
            
        Returns:
            Reset configuration dictionary or None if not found
        """
        try:
            then_section = rule.get('then', {})
            evaluation = then_section.get('evaluation', {})
            
            # Check if this is a roll-back-to-zero rule
            if evaluation.get('basis') != 'ROLL_BACK_TO_ZERO':
                return None
            
            reset_config = evaluation.get('reset_config', {})
            
            # If reset_config is empty, return None (backward compatibility)
            if not reset_config:
                return None
            
            # Validate and return reset_config
            reset_type = reset_config.get('type', 'TIME_BASED')
            period = reset_config.get('period', 'MONTHLY')
            
            # Validate reset_type
            if reset_type not in ['TIME_BASED', 'THRESHOLD_BASED', 'SEASONAL']:
                logger.warning(f"Invalid reset_type: {reset_type}, defaulting to TIME_BASED")
                reset_config['type'] = 'TIME_BASED'
            
            # Validate period for TIME_BASED
            if reset_type == 'TIME_BASED':
                if period not in ['MONTHLY', 'QUARTERLY', 'CUSTOM']:
                    logger.warning(f"Invalid period: {period}, defaulting to MONTHLY")
                    reset_config['period'] = 'MONTHLY'
                
                # Validate custom period values
                if period == 'CUSTOM':
                    if not reset_config.get('reset_after_days') and not reset_config.get('reset_after_months'):
                        logger.warning("CUSTOM period requires reset_after_days or reset_after_months, defaulting to MONTHLY")
                        reset_config['period'] = 'MONTHLY'
            
            return reset_config
            
        except Exception as e:
            logger.error(f"Error parsing reset_config: {str(e)}")
            return None
    
    def _calculate_time_based_periods(self, start_date: date, end_date: date,
                                      period: str, reset_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Calculate period boundaries based on period type
        
        Args:
            start_date: Contract start date
            end_date: Contract end date
            period: Period type (MONTHLY, QUARTERLY, CUSTOM)
            reset_config: Reset configuration dictionary
            
        Returns:
            List of period dictionaries with index, start, and end dates
        """
        periods = []
        current = start_date
        index = 1
        
        try:
            if period == 'MONTHLY':
                while current <= end_date:
                    # Calculate month end
                    if current.month == 12:
                        next_month_start = date(current.year + 1, 1, 1)
                    else:
                        next_month_start = date(current.year, current.month + 1, 1)
                    
                    period_end = next_month_start - timedelta(days=1)
                    
                    # Cap at contract end date
                    if period_end > end_date:
                        period_end = end_date
                    
                    periods.append({
                        'index': index,
                        'start': current,
                        'end': period_end
                    })
                    
                    # Move to next month
                    current = period_end + timedelta(days=1)
                    index += 1
                    
                    # Safety check to prevent infinite loops
                    if index > 1000:
                        logger.error("Too many periods calculated, breaking loop")
                        break
            
            elif period == 'QUARTERLY':
                if relativedelta is None:
                    logger.error("dateutil.relativedelta not available for quarterly calculations")
                    # Fallback: approximate quarters using days
                    quarter_days = 90
                    while current <= end_date:
                        period_end = current + timedelta(days=quarter_days - 1)
                        if period_end > end_date:
                            period_end = end_date
                        
                        periods.append({
                            'index': index,
                            'start': current,
                            'end': period_end
                        })
                        
                        current = period_end + timedelta(days=1)
                        index += 1
                        
                        if index > 100:
                            break
                else:
                    while current <= end_date:
                        # Calculate quarter end (3 months later)
                        period_end = current + relativedelta(months=3) - timedelta(days=1)
                        
                        if period_end > end_date:
                            period_end = end_date
                        
                        periods.append({
                            'index': index,
                            'start': current,
                            'end': period_end
                        })
                        
                        current = period_end + timedelta(days=1)
                        index += 1
                        
                        if index > 100:
                            break
            
            elif period == 'CUSTOM':
                # Check for days or months
                if reset_config.get('reset_after_days'):
                    days = int(reset_config['reset_after_days'])
                    while current <= end_date:
                        period_end = current + timedelta(days=days - 1)
                        if period_end > end_date:
                            period_end = end_date
                        
                        periods.append({
                            'index': index,
                            'start': current,
                            'end': period_end
                        })
                        
                        current = period_end + timedelta(days=1)
                        index += 1
                        
                        if index > 1000:
                            break
                
                elif reset_config.get('reset_after_months'):
                    months = int(reset_config['reset_after_months'])
                    if relativedelta is None:
                        logger.error("dateutil.relativedelta not available for custom month calculations")
                        # Fallback: approximate using days
                        days = months * 30
                        while current <= end_date:
                            period_end = current + timedelta(days=days - 1)
                            if period_end > end_date:
                                period_end = end_date
                            
                            periods.append({
                                'index': index,
                                'start': current,
                                'end': period_end
                            })
                            
                            current = period_end + timedelta(days=1)
                            index += 1
                            
                            if index > 100:
                                break
                    else:
                        while current <= end_date:
                            period_end = current + relativedelta(months=months) - timedelta(days=1)
                            if period_end > end_date:
                                period_end = end_date
                            
                            periods.append({
                                'index': index,
                                'start': current,
                                'end': period_end
                            })
                            
                            current = period_end + timedelta(days=1)
                            index += 1
                            
                            if index > 100:
                                break
            
            return periods
            
        except Exception as e:
            logger.error(f"Error calculating time-based periods: {str(e)}")
            return []
    
    def _generate_mtps_for_rollback(self, rule: Dict[str, Any], metadata: Dict[str, Any],
                                    start_date: date, end_date: date,
                                    filename: str, reset_config: Dict[str, Any]) -> List[ContractData]:
        """
        Generate multiple ContractData objects (MTPs) based on reset_config
        
        Args:
            rule: Original rule data
            metadata: Rule metadata
            start_date: Contract start date
            end_date: Contract end date
            filename: Source filename
            reset_config: Reset configuration dictionary
            
        Returns:
            List of ContractData objects (one per period)
        """
        contracts = []
        
        try:
            reset_type = reset_config.get('type', 'TIME_BASED')
            period = reset_config.get('period', 'MONTHLY')
            mtp_naming = reset_config.get('mtp_naming', 'SEQUENTIAL')
            generate_mtps = reset_config.get('generate_mtps', False)
            
            if not generate_mtps:
                # If generate_mtps is False, return single MTP
                contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, 1)
                if contract:
                    contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                    contract.reset_config = reset_config
                    contracts.append(contract)
                return contracts
            
            if reset_type == 'TIME_BASED':
                # Calculate periods
                periods = self._calculate_time_based_periods(start_date, end_date, period, reset_config)
                
                if not periods:
                    logger.warning("No periods calculated, falling back to single MTP")
                    contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, 1)
                    if contract:
                        contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                        contract.reset_config = reset_config
                        contracts.append(contract)
                    return contracts
                
                # Generate ContractData for each period
                for period_info in periods:
                    mtp_rule = rule.copy()
                    period_index = period_info['index']
                    period_start = period_info['start']
                    period_end = period_info['end']
                    
                    # Generate MTP ID - extract base ID (remove existing _MTP suffix if present)
                    base_rule_id = rule.get('rule_id', 'MTP')
                    # Remove any existing _MTP suffix (e.g., "RULE_MTP1" -> "RULE", "RULE_MTP1_MTP2" -> "RULE")
                    if '_MTP' in base_rule_id:
                        # Find the last _MTP occurrence and remove everything after it
                        last_mtp_pos = base_rule_id.rfind('_MTP')
                        if last_mtp_pos > 0:
                            base_rule_id = base_rule_id[:last_mtp_pos]
                    
                    if mtp_naming == 'DATE_BASED':
                        mtp_rule['rule_id'] = f"{base_rule_id}_{period_start.strftime('%Y-%m')}"
                    else:
                        mtp_rule['rule_id'] = f"{base_rule_id}_MTP{period_index}"
                    
                    # Update active_window
                    if 'what_if' not in mtp_rule:
                        mtp_rule['what_if'] = {}
                    mtp_rule['what_if']['active_window'] = {
                        'from': period_start.isoformat(),
                        'to': period_end.isoformat()
                    }
                    
                    # Update rule name to include period
                    if period_index == 1:
                        # Keep original name for first MTP
                        pass
                    else:
                        original_name = mtp_rule.get('name', '')
                        if mtp_naming == 'DATE_BASED':
                            period_name = period_start.strftime('%B %Y')
                        else:
                            period_name = f"Period {period_index}"
                        mtp_rule['name'] = f"{original_name} - {period_name}"
                    
                    # Parse this MTP
                    contract = self._parse_single_rule(
                        mtp_rule, metadata, period_start, period_end, filename, period_index
                    )
                    
                    if contract:
                        # Set period-specific fields
                        contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                        contract.reset_config = reset_config
                        contract.mtp_period_index = period_index
                        contract.period_start_date = period_start
                        contract.period_end_date = period_end
                        contracts.append(contract)
            
            elif reset_type == 'THRESHOLD_BASED':
                # Single MTP that will reset when threshold reached
                contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, 1)
                if contract:
                    contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                    contract.reset_config = reset_config
                    contract.mtp_period_index = 1
                    contract.period_start_date = start_date
                    contract.period_end_date = end_date
                    contracts.append(contract)
            
            else:
                # Unknown reset type, fall back to single MTP
                logger.warning(f"Unknown reset_type: {reset_type}, using single MTP")
                contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, 1)
                if contract:
                    contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                    contract.reset_config = reset_config
                    contracts.append(contract)
            
            logger.info(f"Generated {len(contracts)} MTPs for roll-back-to-zero contract")
            return contracts
            
        except Exception as e:
            logger.error(f"Error generating MTPs for rollback: {str(e)}")
            # Fallback to single MTP on error
            contract = self._parse_single_rule(rule, metadata, start_date, end_date, filename, 1)
            if contract:
                contract.evaluation_basis = 'ROLL_BACK_TO_ZERO'
                contract.reset_config = reset_config
                contracts.append(contract)
            return contracts
    
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
