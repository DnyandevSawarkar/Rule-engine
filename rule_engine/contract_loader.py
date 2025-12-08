"""
Contract loading and discovery functionality
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


class ContractLoader:
    """
    Handles loading and parsing of contract JSON files
    """
    
    def __init__(self, contracts_dir: str = "contracts"):
        """
        Initialize contract loader
        
        Args:
            contracts_dir: Directory containing contract JSON files
        """
        self.contracts_dir = Path(contracts_dir)
        self._contracts_cache = {}
        self._last_scan = None
        
        if not self.contracts_dir.exists():
            logger.warning(f"Contracts directory {self.contracts_dir} does not exist")
            self.contracts_dir.mkdir(parents=True, exist_ok=True)
    
    def load_all_contracts(self) -> List[ContractData]:
        """
        Load all contracts from the contracts directory
        
        Returns:
            List of ContractData objects
        """
        contracts = []
        
        try:
            # Scan for JSON files
            json_files = list(self.contracts_dir.glob("*.json"))
            logger.info(f"Found {len(json_files)} contract files")
            
            for json_file in json_files:
                try:
                    contract = self._load_contract_file(json_file)
                    if contract:
                        contracts.append(contract)
                        logger.debug(f"Loaded contract: {contract.contract_name}")
                except Exception as e:
                    logger.error(f"Failed to load contract from {json_file}: {str(e)}")
                    continue
            
            logger.info(f"Successfully loaded {len(contracts)} contracts")
            return contracts
            
        except Exception as e:
            logger.error(f"Error loading contracts: {str(e)}")
            raise ContractError(f"Failed to load contracts: {str(e)}")
    
    def load_contract_by_id(self, contract_id: str) -> Optional[ContractData]:
        """
        Load a specific contract by ID
        
        Args:
            contract_id: Contract identifier
            
        Returns:
            ContractData object or None if not found
        """
        contracts = self.load_all_contracts()
        
        for contract in contracts:
            if contract.contract_id == contract_id:
                return contract
        
        logger.warning(f"Contract with ID {contract_id} not found")
        return None
    
    def load_contracts_for_airline(self, airline_code: str) -> List[ContractData]:
        """
        Load contracts applicable to a specific airline
        
        Args:
            airline_code: Airline code (e.g., "QR", "LH")
            
        Returns:
            List of applicable ContractData objects
        """
        all_contracts = self.load_all_contracts()
        applicable_contracts = []
        
        for contract in all_contracts:
            if self._is_contract_applicable_to_airline(contract, airline_code):
                applicable_contracts.append(contract)
        
        logger.info(f"Found {len(applicable_contracts)} contracts for airline {airline_code}")
        return applicable_contracts
    
    def _load_contract_file(self, file_path: Path) -> Optional[ContractData]:
        """
        Load a single contract file
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            ContractData object or None if parsing fails
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return self._parse_contract_data(data, file_path.name)
            
        except Exception as e:
            logger.error(f"Error loading contract file {file_path}: {str(e)}")
            return None
    
    def _parse_contract_data(self, data: Dict[str, Any], filename: str) -> Optional[ContractData]:
        """
        Parse contract JSON data into ContractData object
        
        Args:
            data: Parsed JSON data
            filename: Source filename
            
        Returns:
            ContractData object or None if parsing fails
        """
        try:
            # Extract document header
            doc_header = data.get('Document_Header', {})
            
            # Extract MTP data (assuming single MTP for now)
            mtp_key = next((k for k in data.keys() if k.startswith('MTP')), 'MTP1')
            mtp_data = data.get(mtp_key, {})
            
            # Parse dates
            start_date = self._parse_date(doc_header.get('Start Date', '2025-01-01'))
            end_date = self._parse_date(doc_header.get('End Date', '2025-12-31'))
            
            # Extract trigger configuration
            trigger_config = mtp_data.get('trigger', {})
            trigger_components = trigger_config.get('trigger_components', ['BASE'])
            trigger_eligibility = trigger_config.get('trigger_eligibility_criteria', {})
            
            # Extract payout configuration
            payout_config = mtp_data.get('payout', {})
            payout_components = payout_config.get('payout_components', ['BASE'])
            payout_eligibility = payout_config.get('payout_eligibility_criteria', {})
            
            # Calculate payout percentage from tiers
            payout_percentage = self._calculate_payout_percentage(mtp_data.get('Tier', []))
            
            # Extract tiers
            tiers = self._extract_tiers(mtp_data.get('Tier', []))
            
            # Generate IDs
            document_id = f"DOC_{datetime.now().strftime('%Y%m%d')}_{filename.replace('.json', '')}"
            contract_id = f"MTP_{document_id}_1"
            rule_id = f"RULE_{contract_id}_001"
            
            contract = ContractData(
                document_name=doc_header.get('Name', filename),
                document_id=document_id,
                contract_name=mtp_data.get('MTP_name', 'Unknown Contract'),
                contract_id=contract_id,
                rule_id=rule_id,
                start_date=start_date,
                end_date=end_date,
                trigger_type=trigger_config.get('type', 'FLOWN'),
                trigger_components=trigger_components,
                trigger_eligibility_criteria=trigger_eligibility,
                payout_type=payout_config.get('type', 'PERCENTAGE'),
                payout_components=payout_components,
                payout_percentage=payout_percentage,
                payout_eligibility_criteria=payout_eligibility,
                tiers=tiers,
                creation_date=datetime.now().date(),
                update_date=datetime.now().date(),
                iata_codes=doc_header.get('IATA', []),
                countries=doc_header.get('Countries', [])
            )
            
            return contract
            
        except Exception as e:
            logger.error(f"Error parsing contract data: {str(e)}")
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
    
    def _calculate_payout_percentage(self, tiers: List[Dict[str, Any]]) -> Optional[Decimal]:
        """
        Calculate average payout percentage from tiers
        
        Args:
            tiers: List of tier data
            
        Returns:
            Average payout percentage or None
        """
        if not tiers:
            return None
        
        total_percentage = Decimal('0')
        count = 0
        
        for tier in tiers:
            rows = tier.get('rows', [])
            for row in rows:
                payout_value = row.get('payout_value', 0)
                payout_unit = row.get('payout_unit', '')
                
                if payout_unit == 'PERCENT':
                    total_percentage += Decimal(str(payout_value))
                    count += 1
        
        if count > 0:
            return total_percentage / count
        
        return None
    
    def _extract_tiers(self, tier_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract and normalize tier information
        
        Args:
            tier_data: Raw tier data from contract
            
        Returns:
            Normalized tier data
        """
        tiers = []
        
        for tier in tier_data:
            normalized_tier = {
                'table_label': tier.get('table_label', 'Total Target Revenue'),
                'table_filter': tier.get('table_filter', {'key': 'NONE', 'value': ['NONE']}),
                'rows': []
            }
            
            for row in tier.get('rows', []):
                normalized_row = {
                    'metric': row.get('metric', 'Net Flown Revenue'),
                    'target_min': row.get('target_min', 0),
                    'target_max': row.get('target_max', 0),
                    'payout_value': row.get('payout_value', 0),
                    'payout_unit': row.get('payout_unit', 'PERCENT')
                }
                normalized_tier['rows'].append(normalized_row)
            
            tiers.append(normalized_tier)
        
        return tiers
    
    def _is_contract_applicable_to_airline(self, contract: ContractData, airline_code: str) -> bool:
        """
        Check if contract is applicable to specific airline
        
        Args:
            contract: Contract data
            airline_code: Airline code to check
            
        Returns:
            True if applicable, False otherwise
        """
        # Check trigger eligibility criteria
        trigger_criteria = contract.trigger_eligibility_criteria
        in_criteria = trigger_criteria.get('IN', {})
        
        # Check marketing airline
        marketing_airlines = in_criteria.get('Marketing Airline', [])
        if marketing_airlines and airline_code not in marketing_airlines:
            return False
        
        # Check operating airline
        operating_airlines = in_criteria.get('Operating Airline', [])
        if operating_airlines and airline_code not in operating_airlines:
            return False
        
        # Check ticketing airline
        ticketing_airlines = in_criteria.get('Ticketing Airline', [])
        if ticketing_airlines and airline_code not in ticketing_airlines:
            return False
        
        return True
    
    def get_contract_summary(self) -> Dict[str, Any]:
        """
        Get summary of all loaded contracts
        
        Returns:
            Dictionary with contract summary
        """
        contracts = self.load_all_contracts()
        
        summary = {
            'total_contracts': len(contracts),
            'contracts_by_airline': {},
            'contracts_by_document': {},
            'date_ranges': [],
            'contract_types': set()
        }
        
        for contract in contracts:
            # Group by airline
            airline = contract.document_name.split('_')[0] if '_' in contract.document_name else 'Unknown'
            if airline not in summary['contracts_by_airline']:
                summary['contracts_by_airline'][airline] = 0
            summary['contracts_by_airline'][airline] += 1
            
            # Group by document
            doc_name = contract.document_name
            if doc_name not in summary['contracts_by_document']:
                summary['contracts_by_document'][doc_name] = 0
            summary['contracts_by_document'][doc_name] += 1
            
            # Collect date ranges
            summary['date_ranges'].append({
                'start': contract.start_date.isoformat(),
                'end': contract.end_date.isoformat(),
                'contract_name': contract.contract_name
            })
            
            # Collect contract types
            summary['contract_types'].add(contract.payout_type)
        
        summary['contract_types'] = list(summary['contract_types'])
        
        return summary
