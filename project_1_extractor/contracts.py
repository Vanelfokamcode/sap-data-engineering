from pydantic import BaseModel, field_validator
from typing import Optional

class BusinessPartnerContract(BaseModel):
    business_partner: str
    full_name: Optional[str] = None
    bp_category: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = None
    creation_date: Optional[str] = None

    @field_validator("business_partner")
    @classmethod
    def bp_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError("BusinessPartner ne peut pas etre vide")
        return v.strip()

    @field_validator("bp_category")
    @classmethod
    def valid_category(cls, v):
        # SAP : 1=Personne, 2=Organisation, 3=Groupe
        if v and v not in ["1", "2", "3"]:
            raise ValueError(f"Categorie invalide: {v}")
        return v

# contracts.py — partie 2/2 : GLAccountContract (suite du même heredoc)
class GLAccountContract(BaseModel):
    chart_of_accounts: str
    gl_account: str
    gl_account_name: Optional[str] = None
    is_balance_sheet_account: bool = False
    gl_account_group: Optional[str] = None

    @field_validator("chart_of_accounts")
    @classmethod
    def valid_chart(cls, v):
        # SAP : plan comptable = 4 caracteres max
        if len(v) > 4:
            raise ValueError(f"ChartOfAccounts trop long: {v}")
        return v

    @field_validator("is_balance_sheet_account", mode="before")
    @classmethod
    def parse_sap_bool(cls, v):
        # SAP retourne "X" pour True, "" ou None pour False
        if isinstance(v, str):
            return v == "X"
        return bool(v)
