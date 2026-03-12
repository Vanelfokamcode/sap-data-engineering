from pydantic import BaseModel, field_validator
from typing import Optional
from decimal import Decimal


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

class JournalEntryItemContract(BaseModel):
    """Contrat pour une ligne d'écriture FICO."""
    company_code:                str
    ledger_fiscal_year:          str
    journal_entry:               str
    journal_entry_item:          str
    gl_account:                  Optional[str] = None
    amount_in_company_code_currency: Optional[Decimal] = None
    company_code_currency:       Optional[str] = None
    debit_credit_code:           Optional[str] = None
    ledger:                      Optional[str] = None
    cost_center:                 Optional[str] = None

    @field_validator("company_code")
    @classmethod
    def valid_company_code(cls, v):
        if len(v) > 4:
            raise ValueError(f"CompanyCode trop long: {v}")
        return v

    @field_validator("debit_credit_code")
    @classmethod
    def valid_dc_code(cls, v):
        # SAP: "H" = Haben (Crédit), "S" = Soll (Débit)
        if v and v not in ["H", "S", ""]:
            raise ValueError(f"DebitCredit invalide: {v}")
        return v

    @field_validator("amount_in_company_code_currency", mode="before")
    @classmethod
    def parse_amount(cls, v):
        if v is None or v == "": return None
        try: return Decimal(str(v))
        except: return None