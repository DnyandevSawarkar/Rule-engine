"""
Data models for the Rule Engine
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field, validator
import json
import pandas as pd


class CouponData(BaseModel):
    """Model for coupon data input"""
    
    # Core identifiers
    source_system: str = ""
    pcc: str = ""
    ticket_number: Union[str, int, float] = ""
    coupon_number: str = ""
    
    # Airline and flight details
    cpn_airline_code: str = ""
    cpn_fare_basis: str = ""
    cpn_RBD: str = ""
    iata: Optional[str] = ""
    cabin: str = "Unknown"
    
    # Dates
    cpn_sales_date: Union[date, str] = "2025-01-01"
    cpn_flown_date: Union[date, str] = "2025-01-01"
    
    # Geography
    cpn_origin: str = ""
    cpn_destination: str = ""
    
    # Revenue components
    cpn_revenue_base: Decimal = Decimal('0')
    cpn_revenue_yq: Decimal = Decimal('0')
    cpn_revenue_yr: Decimal = Decimal('0')
    cpn_revenue_xt: Decimal = Decimal('0')
    cpn_total_revenue: Decimal = Decimal('0')
    
    # Flight details
    flight_number: Union[str, int, float] = ""
    airline_name: str = "Unknown"
    marketing_airline: Optional[str] = ""
    ticketing_airline: Optional[str] = ""
    corporate_code: Optional[str] = ""
    operating_airline: Optional[str] = ""
    city_codes: Optional[str] = ""
    route: Optional[str] = ""
    
    # Itinerary strings (for enhanced route matching)
    coupon_itinerary: Optional[str] = ""
    ticket_itinerary: Optional[str] = ""
    
    # Technical flags
    code_share: Optional[str] = ""
    interline: Optional[str] = ""
    ndc: Optional[str] = ""
    tour_codes: Optional[str] = ""
    fare_type: Optional[str] = ""
    cpn_is_international: Optional[bool] = False
    
    # New Array fields for enhanced matching
    # Using Any to bypass strict Pydantic type checking for numpy arrays before validator runs
    ticket_origin: Any = ""
    ticket_destination: Any = ""
    ond_array: List[str] = Field(default_factory=list)
    pos_array: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True

    @validator('ticket_origin', 'ticket_destination', pre=True)
    def handle_arrays_for_string_fields(cls, v):
        """Handle numpy arrays or lists passed as values for string fields"""
        if v is None:
            return ""
            
        # Robust handling for numpy arrays without importing numpy
        type_name = type(v).__name__
        
        # Handle numpy array (check by name or tolist attribute)
        if type_name == 'ndarray' or hasattr(v, 'tolist'):
            try:
                # If it's a 0-d array, tolist returns the scalar
                # If it's >0-d array, tolist returns list
                v_list = v.tolist()
                
                # Recursively handle the result if it's still a list/array structure
                if isinstance(v_list, (list, tuple)):
                    if len(v_list) > 0:
                        return cls.handle_arrays_for_string_fields(v_list[0])
                    return ""
                return str(v_list)
            except Exception:
                # If tolist fails, try string conversion of the first element if iterable
                pass
            
        # Handle list/tuple
        if isinstance(v, (list, tuple)):
            if len(v) > 0:
                # Recursively handle nested arrays if validation failed on deep structure
                val = v[0]
                if isinstance(val, (list, tuple)) or hasattr(val, 'tolist') or type(val).__name__ == 'ndarray':
                     return cls.handle_arrays_for_string_fields(val)
                return str(val)
            return ""
            
        return str(v) if v is not None else ""

    
    @validator('ond_array', 'pos_array', pre=True)
    def parse_array_fields(cls, v):
        """Parse string representation of list to actual list"""
        if v is None:
            return []
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            try:
                # Try JSON parsing first (e.g. '["A", "B"]')
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(i) for i in parsed]
                return [v]
            except json.JSONDecodeError:
                # Fallback: Treat as comma-separated or single value
                if ',' in v and '[' not in v:
                    return [i.strip() for i in v.split(',')]
                return [v]
        return [str(v)]
    
    @validator('ticket_number', 'flight_number', pre=True)
    def convert_to_string(cls, v):
        if v is None:
            return ""
        return str(v)
    
    @validator('cpn_sales_date', 'cpn_flown_date', pre=True)
    def parse_dates(cls, v):
        if isinstance(v, str):
            # Handle various date formats including airline industry formats
            date_formats = [
                # ISO formats
                '%Y-%m-%d', 
                '%Y-%m-%dT%H:%M:%S.%fZ', 
                '%Y-%m-%dT%H:%M:%S.%f%z', 
                '%Y-%m-%dT%H:%M',
                # Airline industry formats
                '%d%b%y',  # 30MAY25
                '%d%b',    # 28JUN (assumes current year)
                '%d%B%y',  # 30MAY25 (full month name)
                '%d%B',    # 28JUN (full month name, current year)
            ]
            
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(v, fmt).date()
                    # For formats without year (like 28JUN), assume current year
                    if fmt in ['%d%b', '%d%B']:
                        current_year = datetime.now().year
                        parsed_date = parsed_date.replace(year=current_year)
                    return parsed_date
                except ValueError:
                    continue
            
            # If all formats fail, try to handle common variations
            try:
                # Handle cases like "30MAY25" with different separators or case
                import re
                # Extract day, month, year from patterns like "30MAY25", "28JUN"
                match = re.match(r'(\d{1,2})([A-Za-z]{3,9})(\d{2})?', v.upper())
                if match:
                    day = int(match.group(1))
                    month_str = match.group(2)
                    year_str = match.group(3)
                    
                    # Map month abbreviations
                    month_map = {
                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
                        'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4, 'MAY': 5, 'JUNE': 6,
                        'JULY': 7, 'AUGUST': 8, 'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
                    }
                    
                    if month_str in month_map:
                        month = month_map[month_str]
                        if year_str:
                            # Convert 2-digit year to 4-digit (assume 20xx for years 00-99)
                            year = 2000 + int(year_str)
                        else:
                            # No year provided, use current year
                            year = datetime.now().year
                        
                        return date(year, month, day)
            except:
                pass
            
            # If all parsing attempts fail, return default date
            return date(2025, 1, 1)
        return v
    
    @validator('cpn_revenue_base', 'cpn_revenue_yq', 'cpn_revenue_yr', 
              'cpn_revenue_xt', 'cpn_total_revenue', pre=True)
    def parse_decimal(cls, v):
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return Decimal('0')
        if isinstance(v, (int, float, str)):
            try:
                return Decimal(str(v))
            except:
                return Decimal('0')
        return v
    
    @validator('iata', 'marketing_airline', 'ticketing_airline', 'corporate_code', 
              'operating_airline', 'city_codes', 'route', 'code_share', 'interline', 
              'ndc', 'tour_codes', 'fare_type', 'coupon_itinerary', 'ticket_itinerary', pre=True)
    def handle_empty_strings(cls, v):
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)
    
    @validator('cabin', 'airline_name', pre=True)
    def handle_nan_strings(cls, v):
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return "Unknown"
        return str(v)


class ContractWindow(BaseModel):
    """Contract validity window"""
    start: date
    end: date


class ContractAnalysis(BaseModel):
    """Analysis result for a single contract"""
    
    # Document metadata
    document_name: str
    document_id: str
    contract_name: str
    contract_id: str
    rule_id: str
    
    # Contract window
    contract_window_date: ContractWindow
    
    # Formulas and values
    trigger_formula: str
    trigger_value: Decimal
    payout_formula: str
    payout_value: Decimal
    
    # Eligibility status
    sector_eligibility: bool = False
    trigger_eligibility: bool
    payout_eligibility: bool
    
    # Eligibility reasons
    sector_eligibility_reason: str = ""
    trigger_eligibility_reason: str = ""
    payout_eligibility_reason: str = ""
    
    # New fields for dynamic rule loading
    ruleset_id: Optional[str] = None
    source_name: Optional[str] = None
    currency: str = "USD"
    
    # Rule management
    rule_creation_date: date
    rule_update_date: date


class ProcessingResult(BaseModel):
    """Complete processing result for a coupon"""
    
    # Original coupon data
    coupon_data: CouponData
    
    # Airline eligibility
    airline_eligibility: bool
    
    # Contract analyses
    contract_analyses: Dict[str, ContractAnalysis] = Field(default_factory=dict)
    
    # Processing metadata
    processed_at: datetime = Field(default_factory=datetime.now)
    processing_time_ms: Optional[int] = None
    total_contracts_processed: int = 0
    eligible_contracts: int = 0


class ContractData(BaseModel):
    """Internal contract data structure"""
    
    document_name: str
    document_id: str
    contract_name: str
    contract_id: str
    rule_id: str
    
    # New fields for dynamic rule loading
    ruleset_id: Optional[str] = None
    source_name: Optional[str] = None
    
    # Contract window
    start_date: date
    end_date: date
    
    # Trigger configuration
    trigger_type: str  # "FLOWN" or "SALES"
    trigger_components: List[str]
    trigger_formula: Optional[str] = None
    trigger_eligibility_criteria: Dict[str, Any]
    
    # Payout configuration
    payout_type: str  # "PERCENTAGE" or "FIXED"
    payout_components: List[str]
    payout_formula: Optional[str] = None
    payout_percentage: Optional[Decimal] = None
    payout_eligibility_criteria: Dict[str, Any]
    
    # Financial details
    currency: str = "USD"
    
    # Tier information
    tiers: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Rule management
    creation_date: date
    update_date: date
    
    # IATA and geographic constraints
    iata_codes: List[str] = Field(default_factory=list)
    countries: List[str] = Field(default_factory=list)
    
    # Addon rule cases for special eligibility overrides
    addon_rule_cases: List[Dict[str, Any]] = Field(default_factory=list)