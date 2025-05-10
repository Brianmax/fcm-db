import json
from tqdm import tqdm
import re
import pandas as pd
import uuid



def transform_external_companies_encompass_to_dataframe(data):
    records = []
    for item in data:
        basic_info = item.get("basicInfo", {})
        address = basic_info.get("address", {})
        billing_address = basic_info.get("billingAddress", {})
        approval_status = basic_info.get("approvalStatus", {})
        business_info = basic_info.get("businessInformation", {})
        sales_rep = basic_info.get("primarySalesRepAe", {})
        records.append({
            "ID": basic_info.get("id"),
            "Organization Name": basic_info.get("organizationName"),
            "Is Login Disabled": basic_info.get("isLoginDisabled"),
            "Is MFA Enabled": basic_info.get("isMfaEnabled"),
            "Number of Users": basic_info.get("numberOfUsers"),
            "Hierarchy Path": basic_info.get("hierarchyPath"),
            "Organization Type": basic_info.get("organizationType"),
            "Channel Types": ", ".join(basic_info.get("channelTypes", [])),
            "TPO ID": basic_info.get("tpoId"),
            "Company Legal Name": basic_info.get("companyLegalName"),
            "Address": f"{address.get('street1')}, {address.get('city')}, {address.get('state')} {address.get('zip')}",
            "Billing Address": f"{billing_address.get('street1')}, {billing_address.get('city')}, {billing_address.get('state')} {billing_address.get('zip')}",
            "Phone Number": basic_info.get("phoneNumber"),
            "Last Loan Submitted Date": basic_info.get("lastLoanSubmittedDate"),
            "Approval Status": approval_status.get("currentStatus"),
            "Approved Date": approval_status.get("approvedDate"),
            "Type of Entity": business_info.get("typeOfEntity"),
            "NMLS ID": business_info.get("nmlsId"),
            "Sales Rep Name": sales_rep.get("name"),
            "Sales Rep Email": sales_rep.get("email"),
            "Sales Rep Phone": sales_rep.get("phone"),
            "Assigned Date": sales_rep.get("assignedDate"),
        })

    df = pd.DataFrame(records)
    return df

def transform_internal_companies_encompass_to_dataframe(data):
    records = []
    for item in data:
        org_information = item.get("orgInformation", {})
        nmls = item.get("nmls", {})
        address = org_information.get("address", {})

        records.append({
            "ID": item.get("id"),
            "Organization Name": item.get("name"),
            "Description": item.get("description"),
            "Address": f"{address.get('street1', '')}, {address.get('city', '')}, {address.get('state', '')} {address.get('zip', '')}".strip(", "),
            "Phone": org_information.get("phone"),
            "Fax": org_information.get("fax"),
            "NMLS": nmls.get("code"),

        })

    df = pd.DataFrame(records)
    return df

def map_external_companies_encompass_to_company(api_data):
    companies = []

    for item in api_data:
        basic_info = item.get("basicInfo", {})
        address = basic_info.get("address", {})
        approval_status = basic_info.get("approvalStatus", {})
        sales_rep = basic_info.get("primarySalesRepAe", {})
        business_info = basic_info.get("businessInformation", {})
        organization_name = basic_info.get("organizationName", "")
        status = "DEACTIVATED" if "DEACTIVATED" in organization_name else "ACTIVE"
        billingAddres =  basic_info.get("billingAddress", {})

        company = {
            "id": str(uuid.uuid4()),
            "name": organization_name.replace("DEACTIVATED", "").strip(),
            "is_internal": False,
            "email": basic_info.get("email", "") or "",
            "phone": basic_info.get("phoneNumber", ""),
            "address": f"{address.get('street1', '')}, {address.get('city', '')}, {address.get('state', '')} {address.get('zip', '')}".strip(", "),
            "encompass_id": basic_info.get("id"),
            "last_login": basic_info.get("lastLogin"),
            "status": status,
            "hide_rate": False,
            "tpo_id": basic_info.get("tpoId", ""),
            "owner_name": basic_info.get("companyOwnerName", ""),
            "legal_name": basic_info.get("companyLegalName", ""),
            "billing_address":
                business_info.get("billingAddress",
                                  f"{billingAddres.get('sameAsMainAddress', '')}, {billingAddres.get('street1')}, {billingAddres.get('city')}, {billingAddres.get('state')} {billingAddres.get('zip')}").strip(", "),
            "contact": basic_info.get("companyOwnerName", ""),
            "can_accept_first_payments": business_info.get("canAcceptFirstPayments", False),
            "phone_number": basic_info.get("phoneNumber", ""),
            "website": basic_info.get("website", ""),
            "no_after_hour_wires": business_info.get("noAfterHourWires", False),
        }
        companies.append(company)

    df = pd.DataFrame(companies)
    return df



def map_external_companies_approval_status(uuid_company, api_data):
    basic_info = api_data.get("basicInfo", {})
    approval_status = basic_info.get("approvalStatus", {})

    current_status = approval_status.get("currentStatus", "").upper()
    if current_status == "APPLICATION PENDING":
        current_status = "APPLICATION_PENDING"
    elif current_status == "":
        current_status = None

    approval_status_df = {
        "id": [str(uuid.uuid4())],
        "company_id": [uuid_company],
        "current_status": [current_status],
        "add_to_watchlist": [approval_status.get("addToWatchlist", "")],
        "approved_date": [approval_status.get("approvedDate")]
    }

    return pd.DataFrame(approval_status_df)

def map_external_companies_business_information(uuid_company, api_data):
    basic_info = api_data.get("basicInfo", {})
    business_information = basic_info.get("businessInformation", {})

    typeOfEntity = business_information.get("typeOfEntity", "").upper()

    if typeOfEntity == "LIMITEDLIABILITYCOMPANY":
        typeOfEntity = "LIMITED_LIABILITY_COMPANY"
    elif typeOfEntity == "SOLEPROPRIETORSHIP":
        typeOfEntity = "SOLE_PROPRIETOR_SHIP"
    elif typeOfEntity == "":
        typeOfEntity = None

    bussinesInformation_df= {
        "id" : [str(uuid.uuid4())],
        "company_id": [uuid_company],
        "is_incorporated": [business_information.get("isIncorporated", "")],
        "years_in_business": [business_information.get("yearsInBusiness", "")],
        "tax_id": [business_information.get("taxId", "")],
        "use_ssn_format": [business_information.get("useSsnFormat", "")],
        "lei": [business_information.get("lei", "")],
        "nmls_id": [business_information.get("nmlsId", "")],
        "financials_period": [business_information.get("financialsPeriod", "")],
        "eo_company": [business_information.get("eoCompany", "")],
        "eo_policy_number": [business_information.get("eoPolicyNumber", "")],
        "mers_originating_org_id": [business_information.get("mersOriginatingOrgId", "")],
        "type_of_entity": [typeOfEntity],
    }
    return pd.DataFrame(bussinesInformation_df)

def map_external_commitment(uuid_company, api_data):

    commitment_df = {
        "id": [str(uuid.uuid4())],
        "company_id": [uuid_company],
        "best_effort": [api_data.get("bestEffort")],
        "limited": [api_data.get("limited")],
        "unlimited": [api_data.get("unlimited")],
        "mandatory": [api_data.get("mandatory")],
        "max_commitment_authority": [api_data.get("maxCommitmentAuthority")],
        "best_effort_daily_volume_limit": [api_data.get("bestEffortDailyVolumeLimit")],
        "commitments_loan_policy": [api_data.get("commitmentsLoanPolicy")],
        "commitments_trade_policy": [api_data.get("commitmentsTradePolicy")],
        "best_efforts_daily_limit_policy": [api_data.get("bestEffortsDailyLimitPolicy")],
        "daily_limit_warning_message": [api_data.get("dailyLimitWarningMessage")],
        "commitment_warning_message": [api_data.get("commitmentWarningMessage")],
        "best_effort_tolerance_control_option": [api_data.get("bestEffortToleranceControlOption")],
        "mandatory_tolerance_control_option": [api_data.get("mandatoryToleranceControlOption")],
        "best_effort_tolerance_percent": [api_data.get("bestEffortTolerancePercent")],
        "mandatory_tolerance_percent": [api_data.get("mandatoryTolerancePercent")],
    }

    return pd.DataFrame(commitment_df)

def map_external_late_fee_setting(uuid_company, api_data):
    api_data = api_data.get("lateFeeSettings")
    late_fee_df = {
        "id": [str(uuid.uuid4())],
        "late_fee_setting_id": [api_data.get("lateFeeSettingId")],
        "company_id": [uuid_company],
        "grace_period_days": [api_data.get("gracePeriodDays")],
        "grace_period_uses": [api_data.get("gracePeriodUses")],
        "grace_period_starts": [api_data.get("gracePeriodStarts")],
        "grace_period_later_of": [api_data.get("gracePeriodLaterOf", [])],  # Lista de strings
        "day_cleared": [api_data.get("dayCleared")],
        "can_start_on_weekend": [api_data.get("canStartOnWeekend")],
        "include_day_as_late_day": [api_data.get("includeDayAsLateDay")],
        "fee_handled_as": [api_data.get("feeHandledAs")],
        "late_fee_percent": [api_data.get("lateFeePercent")],
        "late_fee_dollars": [api_data.get("lateFeeDollars")],
        "calculate_as": [api_data.get("calculateAs")],
        "max_late_days": [api_data.get("maxLateDays")],
        "day_cleared_other_date": [api_data.get("dayClearedOtherDate")],  # Puede ser None
        "grace_period_later_of_other_date": [api_data.get("gracePeriodLaterOfOtherDate")],  # Puede ser None
    }

    return pd.DataFrame(late_fee_df)


def map_external_lender_contact(uuid_company, api_data):
    lenderContacts = []
    for data in api_data:
        lender_contact_df = {
            "id": str(uuid.uuid4()),
            "company_id": uuid_company,
            "is_wholesale_channel_enabled": data.get("isWholesaleChannelEnabled"),
            "is_delegated_channel_enabled": data.get("isDelegatedChannelEnabled"),
            "is_non_delegated_channel_enabled": data.get("isNonDelegatedChannelEnabled"),
            "is_primary_sales_rep": data.get("isPrimarySalesRep"),
            "is_hidden": data.get("isHidden"),
            "phone": data.get("phone"),
            "email": data.get("email"),
            "user_id": data.get("userId"),
            "name": data.get("name"),
            "title": data.get("title"),
        }
        lenderContacts.append(lender_contact_df)

    df = pd.DataFrame(lenderContacts).drop_duplicates()
    return df


def map_internal_companies_encompass_to_company(api_data):
    companies = []

    for item in api_data:
        org_information = item.get("orgInformation", {})
        nmls = item.get("nmls", {})
        address = org_information.get("address", {})

        company = {
            "id":uuid.uuid4(),
            "name": item.get("name"),
            "status": "ACTIVE",
            "address": f"{address.get('street1', '')}, {address.get('city', '')}, {address.get('state', '')} {address.get('zip', '')}".strip(", "),
            "contact": None,
            "email": None,
            "phone": org_information.get("phone"),
            "nmls_id": nmls.get("code"),
            "encompass_id": item.get("id"),
            "is_internal": True,
        }
        companies.append(company)

    df = pd.DataFrame(companies)
    return df

def map_rate_lock_info(uuid_company, api_data):
    rateLockInfo = api_data.get("basicInfo").get("rateLockInfo")
    rateLockInfo_df = {
        "id": [str(uuid.uuid4())],
        "use_parent_info_for_rate_lock": [rateLockInfo.get("useParentInfoForRateLock")],
        "rate_sheet_email": [rateLockInfo.get("rateSheetEmail")],
        "rate_sheet_fax": [rateLockInfo.get("rateSheetFax")],
        "lock_info_email": [rateLockInfo.get("lockInfoEmail")],
        "lock_info_fax": [rateLockInfo.get("lockInfoFax")],
        "company_id": [uuid_company],
    }
    return pd.DataFrame(rateLockInfo_df)

def map_external_dba(uuid_company, api_data):
    dbaDetails = api_data.get("dbaDetails", [])
    dbaDetails_df = []
    for detail in dbaDetails:
        dba_df = {
            "id": str(uuid.uuid4()),
            "company_id": uuid_company,
            "external_org_id": detail.get("externalOrgId"),
            "dba_id": detail.get("dbaId"),
            "name": detail.get("name"),
            "is_default": detail.get("isDefault"),
            "sort_index": detail.get("sortIndex"),
        }
        df = pd.DataFrame([dba_df])
        dbaDetails_df.append(df)

    return dbaDetails_df

def map_external_warehouses(uuid_company, warehouses_data):
    warehouses = warehouses_data.get("warehouseBankDetails")
    if warehouses is None:
        warehouses = []
    rows = []

    for warehouse in warehouses:
        row = {
            "id": str(uuid.uuid4()),
            "company_id": uuid_company,
            "warehouse_bank_id": warehouse.get("warehouseBankId"),
            "bank_name": warehouse.get("bankName"),
            "adress1": warehouse.get("adress1"),
            "adress2": warehouse.get("adress2"),
            "city": warehouse.get("city"),
            "state": warehouse.get("state"),
            "zip": warehouse.get("zip"),
            "aba_number": warehouse.get("abaNumber"),
            "date_added": warehouse.get("dateAdded"),
            "status_date": warehouse.get("statusDate"),
            "is_approved": warehouse.get("isApproved", False),
            "account_number": warehouse.get("accountNumber"),
            "account_name": warehouse.get("accountName"),
            "credit_account_number": warehouse.get("creditAccountNumber"),
            "credit_account_name": warehouse.get("creditAccountName"),
            "description": warehouse.get("description"),
            "use_default_contact": warehouse.get("useDefaultContact", False),
            "contact_name": warehouse.get("contactName"),
            "contact_email": warehouse.get("contactEmail"),
            "contact_phone": warehouse.get("contactPhone"),
            "contact_fac": warehouse.get("contactFac"),
            "notes": warehouse.get("notes"),
        }
        df = pd.DataFrame([row])
        rows.append(df)
    return rows

def map_external_tpo_contacts(uuid_company, tpo_contacts_data):
    rows = []

    for contact in tpo_contacts_data:
        row = {
            "id": str(uuid.uuid4()),
            "company_id": uuid_company,
            "tpo_contact_id": contact.get("tpoContactId"),
            "name": contact.get("name"),
            "title": contact.get("title")
        }
        df = pd.DataFrame([row])
        rows.append(df)
    return rows

def map_external_sales_rep_aes(uuid_company, sales_rep_data):
    rows = []

    for rep in sales_rep_data:
        row = {
            "id": str(uuid.uuid4()),
            "company_id": uuid_company,
            "user_id": rep.get("userId"),
            "sales_rep_id": rep.get("salesRepId"),
            "is_primary_sales_rep_ae": rep.get("isPrimarySalesRep", False),
            "is_wholesale_channel_enabled": rep.get("isWholesaleChannelEnabled", False),
            "is_delegated_channel_enabled": rep.get("isDelegatedChannelEnabled", False),
            "is_non_delegated_channel_enabled": rep.get("isNonDelegatedChannelEnabled", False),
            "is_lender_contact": rep.get("isLenderContact", False),
            "is_hidden": rep.get("isHidden", False),
            "name": rep.get("name"),
            "persona": rep.get("persona"),
            "phone": rep.get("phone"),
            "email": rep.get("email"),
            "org_assignment": rep.get("orgAssignment"),
            "title": rep.get("title")
        }
        df = pd.DataFrame([row])
        rows.append(df)

    return rows

def map_external_lo_comp_histories(uuid_company, api_data_list):
    api_data_list = api_data_list.get("loCompHistory", [])

    rows = []
    for item in api_data_list:
        broker = item.get("brokerValue", [])
        brokerValue = []
        if len(broker) > 0:
            brokerValue = [broker[0].upper()]
        row = {
            "id": str(uuid.uuid4()),
            "comp_plan_id": item.get("compPlanId"),
            "name": item.get("name"),
            "description": item.get("description"),
            "trigger_basis": item.get("triggerBasis"),
            "rounding": item.get("rounding"),
            "start_date": item.get("startDate"),
            "end_date": item.get("endDate"),
            "min_term_days": item.get("minTermDays"),
            "percent_amt": item.get("percentAmt"),
            "percent_amt_is_of": item.get("percentAmtIsOf"),
            "amount": item.get("amount"),
            "min_amount": item.get("minAmount"),
            "max_amount": item.get("maxAmount"),
            "company_id": uuid_company,
            "broker_type": brokerValue,
        }
        df = pd.DataFrame([row])
        rows.append(df)

    return rows

def map_external_notes(uuid_company, api_data_list):
    rows = []

    for item in api_data_list:
        row = {
            "note_id":item.get("noteId"),
            "added_date": item.get("addedDate"),
            "added_by": item.get("addedBy"),
            "details": item.get("details"),
            "company_id": uuid_company
        }
        df = pd.DataFrame([row])
        rows.append(df)

    return rows

def map_external_trade_management(uuid_company, api_data):
    row = {
        "id": [str(uuid.uuid4())],
        "enable_trade_management": [api_data.get("enableTradeManagement")],
        "use_company_trade_management_settings": [api_data.get("useCompanyTradeManagementSettings")],
        "view_correspondent_trade": [api_data.get("viewCorrespondentTrade")],
        "view_correspondent_master_commitment": [api_data.get("viewCorrespondentMasterCommitment")],
        "loan_eligibility_to_correspondent_trade": [api_data.get("loanEligibilityToCorrespondentTrade")],
        "epps_loan_program_eligibility_pricing": [api_data.get("eppsLoanProgramEligibilityPricing")],
        "loan_assignment_to_correspondent_trade": [api_data.get("loanAssignmentToCorrespondentTrade")],
        "loan_deletion_from_correspondent_trade": [api_data.get("loanDeletionFromCorrespondentTrade")],
        "request_pair_off": [api_data.get("requestPairOff")],
        "receive_commitment_confirmation": [api_data.get("receiveCommitmentConfirmation")],
        "company_id": [uuid_company]
    }

    return pd.DataFrame(row)


def map_external_primary_sales_rep_ae(uuid_company, api_data):
    persona = api_data.get("persona")
    persona = [p.replace(" ", "_").upper() for p in persona] if isinstance(persona, list) else [persona.upper()]
    row = {
        "id": [str(uuid.uuid4())],
        "user_id": [api_data.get("userId")],
        "name": [api_data.get("name")],
        "persona": [persona],
        "phone": [api_data.get("phone")],
        "email": [api_data.get("email")],
        "assigned_date": [api_data.get("assignedDate")],
        "company_id": [uuid_company]
    }

    return pd.DataFrame(row)

def map_external_license(uuid_company, api_data):
    row = {
        "id": [str(uuid.uuid4())],
        "company_id": [uuid_company],
        "lender_type": [api_data.get("lenderType")],
        "home_state": [api_data.get("homeState")],
        "dont_apply_int_rate_exportation": [api_data.get("dontApplyIntRateExportation", False)],
        "loan_policy_for_unlicesed_states": [api_data.get("loanPolicyForUnlicesedStates")],
        "warning_message": [api_data.get("warningMessage")],
        "statutory_election_in_maryland": [api_data.get("statutoryElectionInMaryland")],
        "statutory_election_in_kansas": [api_data.get("statutoryElectionInKansas")],
        "atr_small_creditor": [api_data.get("atrSmallCreditor")],
        "atr_exempt_creditor": [api_data.get("atrExemptCreditor")]
    }

    return pd.DataFrame(row)

def map_external_license_types(uuid_license, api_data):
    rows = []

    for lt in api_data:
        row = {
            "id": str(uuid.uuid4()),
            "license_id": uuid_license,
            "state_abbreviation": lt.get("stateAbbreviation"),
            "selected": lt.get("selected"),
            "exempted": lt.get("exempted"),
            "license_type": lt.get("licenseType"),
            "license_number": lt.get("licenseNumber")
        }
        rows.append(row)
    return pd.DataFrame(rows)

def map_external_onrp(uuid_company, api_data):

    row = {
        "id": str(uuid.uuid4()),
        "onrpId": api_data.get("onrpId", -1),
        "companyId": uuid_company
    }

    return pd.DataFrame([row])

def map_external_onrp_settings(uuid_onrp, api_data):
    brokerSettings = api_data.get("brokerSettings")
    correspondentSettings = api_data.get("correspondentSettings")
    rows = []

    if brokerSettings is not None:
        rowBrokerSettings = {
            "id": str(uuid.uuid4()),
            "enable_onrp_for_tpo": api_data.get("enableOnrpForTpo", False),
            "use_channel_defaults": api_data.get("useChannelDefaults", False),
            "coverage_setting": api_data.get("coverageSetting"),
            "weekend_holiday_coverage": api_data.get("weekendHolidayCoverage", False),
            "onrp_weekday_start_time": api_data.get("onrpWeekdayStartTime"),
            "onrp_week_day_end_time": api_data.get("onrpWeekDayEndTime"),
            "enable_saturday_hours": api_data.get("enableSaturdayHours", False),
            "enable_sunday_hours": api_data.get("enableSundayHours", False),
            "no_maximum_limit": api_data.get("noMaximumLimit", False),
            "dollar_limit": api_data.get("dollarLimit", 0),
            "tolerance_percent": api_data.get("tolerancePercent", 0),
            "onrp_id": uuid_onrp,
            "settingType": "BROKER"
        }
        rows.append(rowBrokerSettings)
    if correspondentSettings is not None:
        rowBrokerSettings = {
            "id": str(uuid.uuid4()),
            "enable_onrp_for_tpo": api_data.get("enableOnrpForTpo", False),
            "use_channel_defaults": api_data.get("useChannelDefaults", False),
            "coverage_setting": api_data.get("coverageSetting"),
            "weekend_holiday_coverage": api_data.get("weekendHolidayCoverage", False),
            "onrp_weekday_start_time": api_data.get("onrpWeekdayStartTime"),
            "onrp_week_day_end_time": api_data.get("onrpWeekDayEndTime"),
            "enable_saturday_hours": api_data.get("enableSaturdayHours", False),
            "enable_sunday_hours": api_data.get("enableSundayHours", False),
            "no_maximum_limit": api_data.get("noMaximumLimit", False),
            "dollar_limit": api_data.get("dollarLimit", 0),
            "tolerance_percent": api_data.get("tolerancePercent", 0),
            "onrp_id": uuid_onrp,
            "settingType": "CORRESPONDENT"
        }
        rows.append(rowBrokerSettings)

    return pd.DataFrame(rows)

def map_external_custom_fields(uuid_company, api_data):
    rows = []

    for field in api_data:
        row = {
            "id": str(uuid.uuid4()),
            "field_name": field.get("fieldName"),
            "field_type": field.get("fieldType"),
            "companyId": uuid_company
        }
        rows.append(row)

    return pd.DataFrame(rows)

def map_external_loan_criteria(uuid_company, api_data):
    row = {
        "id": str(uuid.uuid4()),
        "company_id": uuid_company,
        "fha_sponsor_id": api_data.get("fhaSponsorId"),
        "fha_compare_ratio": api_data.get("fhaCompareRatio"),
        "va_sponsor_id": api_data.get("vaSponsorId"),
        "fnma_approved": api_data.get("fnmaApproved", False),
        "fhmlc_approved": api_data.get("fhmlcApproved", False)
    }
    return pd.DataFrame([row])
def map_external_broker(uuid_loan_criteria, api_data):
    loanTypes = api_data.get("loanTypes")
    loanPurposes = api_data.get("loanPurposes")
    loanTypes = [splitString(lt) for lt in loanTypes] if isinstance(loanTypes, list) else [loanTypes]
    loanPurposes = [splitString(lp) for lp in loanPurposes] if isinstance(loanPurposes, list) else [loanPurposes]
    row = {
        "id": str(uuid.uuid4()),
        "loan_criteria_id": uuid_loan_criteria,
        "loan_policy_for_unlicesed_states": api_data.get("loanPolicyForUnlicesedStates"),
        "warning_message": api_data.get("warningMessage"),
        "loan_types": loanTypes,
        "loan_purposes": loanPurposes
    }

    return pd.DataFrame([row])

def splitString(s):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).upper()

def map_external_correspondent(uuid_loan_criteria, api_data):
    row = {
        "id": str(uuid.uuid4()),
        "loan_criteria_id": uuid_loan_criteria,
        "underwriting": api_data.get("underwriting"),
        "advanced_code": api_data.get("advancedCode")
    }

    return pd.DataFrame([row])

def map_external_correspondent_settings(uuid_correspondent, api_data):
    correspondentDelegated = api_data.get("correspondentDelegated")
    correspondentNonDelegated = api_data.get("correspondentNonDelegated")

    rows = []
    if correspondentDelegated is not None:
        loanTypes = api_data.get("loanTypes")
        loanPurposes = api_data.get("loanPurposes")
        loanTypes = [splitString(lt) for lt in loanTypes] if isinstance(loanTypes, list) else [loanTypes]
        loanPurposes = [splitString(lp) for lp in loanPurposes] if isinstance(loanPurposes, list) else [loanPurposes]
        row = {
            "id": str(uuid.uuid4()),
            "correspondent_id": uuid_correspondent,
            "loan_policy_for_unlicesed_states": api_data.get("loanPolicyForUnlicesedStates"),
            "warning_message": api_data.get("warningMessage"),
            "loan_types": loanTypes,
            "loan_purposes": loanPurposes,
            "correspondent_type": "CORRESPONDENT_DELEGATED"
        }
        rows.append(row)
    if correspondentNonDelegated is not None:
        loanTypes = api_data.get("loanTypes")
        loanPurposes = api_data.get("loanPurposes")
        loanTypes = [splitString(lt) for lt in loanTypes] if isinstance(loanTypes, list) else [loanTypes]
        loanPurposes = [splitString(lp) for lp in loanPurposes] if isinstance(loanPurposes, list) else [loanPurposes]
        row = {
            "id": str(uuid.uuid4()),
            "correspondent_id": uuid_correspondent,
            "loan_policy_for_unlicesed_states": api_data.get("loanPolicyForUnlicesedStates"),
            "warning_message": api_data.get("warningMessage"),
            "loan_types": loanTypes,
            "loan_purposes": loanPurposes,
            "correspondent_type": "CORRESPONDENT_NON_DELEGATED"
        }
        rows.append(row)

    return pd.DataFrame(rows)


def generate_unique_name(base_name, engine):
    """Genera un nombre único en la base de datos añadiendo un número secuencial."""
    existing_names = pd.read_sql('SELECT name FROM "company-management".companies', engine)["name"].tolist()

    if base_name not in existing_names:
        return base_name

    counter = 1
    new_name = f"{base_name} {counter}"

    while new_name in existing_names:
        counter += 1
        new_name = f"{base_name} {counter}"

    return new_name


GREEN = "\033[92m"
RESET = "\033[0m"

def get_all_complete_companies(client, ids):
    external_companies_complete = []

    for company_id in tqdm(ids, desc=f"{GREEN}Fetching external companies{RESET}"):
        external_company = client.fetch_external_companies_all(company_id)
        if external_company:
            external_companies_complete.append(external_company)

    with open("external_companies.json", "w", encoding="utf-8") as f:
        json.dump(external_companies_complete, f, ensure_ascii=False, indent=4)
    return external_companies_complete