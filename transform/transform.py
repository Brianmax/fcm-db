from numpy import dtype
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql._elements_constructors import text
import pandas as pd
import enum
from sqlalchemy.dialects.postgresql import ENUM, ARRAY
from sqlalchemy.types import Uuid



from utils.utils import *


class BrokerTypeEnum(enum.Enum):
    BROKER = "BROKER"

broker_type_enum = ENUM(
    BrokerTypeEnum,
    name="BrokerType",
    schema="company-management",
    create_type=False
)

class SettingsTypeEnum(enum.Enum):
    BROKER = "BROKER"
    CORRESPONDENT = "CORRESPONDENT"

settings_type_enum = ENUM(
    SettingsTypeEnum,
    name="SettingsType",
    schema="company-management",
    create_type=False
)

class PersonaRolesEnum(enum.Enum):
    LOAN_OFFICER = "LOAN_OFFICER"
    TPO_ACCOUNT_EXCECUTIVE = "TPO_ACCOUNT_EXCECUTIVE"
    TPO_LOAN_OFFICER = "TPO_LOAN_OFFICER"

persona_roles_enum = ENUM(
    PersonaRolesEnum,
    name="PersonaRoles",
    schema="company-management",
    create_type=False
)

class LoanTypesEnum(enum.Enum):
    CONVENTIONAL = "CONVENTIONAL"
    FHA = "FHA"
    VA = "VA"
    USDA = "USDA"
    FIRST_LIEN = "FIRST_LIEN"
    OTHER = "OTHER"

class LoanPurposesEnum(enum.Enum):
    PURCHASE = "PURCHASE"
    NO_CASHOUT_REFI = "NO_CASHOUT_REFI"
    CASHOUT_REFI = "CASHOUT_REFI"
    CONSTRUCTION_PERM = "CONSTRUCTION_PERM"
    OTHER = "OTHER"
    NO_CASH_OUT_REFI: "NO_CASH_OUT_REFI"
    CASH_OUT_REFI: "CASH_OUT_REFI"

loan_types_enum = ENUM(
    LoanTypesEnum,
    name="LoanTypes",
    schema="company-management",
    create_type=False
)

loan_purposes_enum = ENUM(
    LoanPurposesEnum,
    name="LoanPurposes",
    schema="company-management",
    create_type=False
)

def insert_companies_to_db(companies_df, database_url, external_companies, completeExternalCompanies):
    errors = 0
    try:
        engine = create_engine(database_url)

        existing_records = pd.read_sql('SELECT encompass_id, is_internal FROM "company-management".companies', engine)

        existing_records_set = set(existing_records[["encompass_id", "is_internal"]].itertuples(index=False, name=None))
        for index, row in companies_df.iterrows():
            current_record = (str(row["encompass_id"]), row["is_internal"])
            if current_record in existing_records_set:
                print(f"🟡 Fila {index} omitida: name -> {row['name']}, encompass_id -> {row['encompass_id']}, isInternal -> {row['is_internal']} ya existe.")
                continue

            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql('companies', engine,schema="company-management", if_exists="append", index=False)
                print(f"✅ Fila {index} insertada correctamente: {row['name']} ---- Encompass id {row['encompass_id']}, isInternal -> {row['is_internal']}")
                with engine.connect() as conn:
                    result = conn.execute(
                        text("""
                             SELECT id FROM "company-management".companies
                             WHERE name = :name
                             ORDER BY created_at DESC
                                 LIMIT 1
                             """),
                        {"name": row['name']}
                    )

                json = external_companies[index]
                uuid_company = result.scalar()
                completeInfo = completeExternalCompanies[index]
                approvalStatus = map_external_companies_approval_status(uuid_company, json)
                approvalStatus.to_sql('approval_statuses', engine, if_exists="append", index=False, schema="company-management")
                bussinesInformation = map_external_companies_business_information(uuid_company, json)
                bussinesInformation.to_sql('bussiness_informations', engine, if_exists="append", index=False, schema="company-management")
                commitments = map_external_commitment(uuid_company, completeInfo.get("commitments"))
                commitments.to_sql('commitments', engine, if_exists="append", index=False, schema="company-management")
                fees = map_external_late_fee_setting(uuid_company, completeInfo.get("fees"))
                fees.to_sql('fees', engine, if_exists="append", index=False, schema="company-management")
                #insert lenderContacts
                # lenderContacts = map_external_lender_contact(uuid_company, completeInfo.get("lenderContacts"))
                # print(f"lenderContacts: {len(lenderContacts)}")
                # for lenderContact in lenderContacts:
                #    lenderContact.to_sql("lender-contacts",engine, if_exists="append", index=False, schema="company-management")
                # lenderContacts.clear()

                # Insert rate lock info
                rateLockInfo = map_rate_lock_info(uuid_company, json)
                rateLockInfo.to_sql('RateLockInfo', engine, if_exists="append", index=False, schema="company-management")

                # insert dbas
                dbas = map_external_dba(uuid_company, completeInfo.get("dba"))
                for dba in dbas:
                    dba.to_sql("dbas", engine, if_exists="append", index=False, schema="company-management")

                # insert warehouse
                warehouses = map_external_warehouses(uuid_company, completeInfo.get("warehouse"))
                for warehouse in warehouses:
                    warehouse.to_sql("warehouses", engine, if_exists="append", index=False, schema="company-management")

                # insert tpoContacts

                tpoContacts = map_external_tpo_contacts(uuid_company, completeInfo.get("tpoContacts"))
                for tpoContact in tpoContacts:
                    tpoContact.to_sql("tpo_contacts", engine, if_exists="append", index=False, schema="company-management")

                # insert sales rep
                #salesRep = map_external_sales_rep_aes(uuid_company, completeInfo.get("salesRepAe"))
                #for salesRep in salesRep:
                    #salesRep.to_sql("sales_rep_aes", engine, if_exists="append", index=False, schema="company-management")

                # insert locomphistory
                loCompHistory = map_external_lo_comp_histories(uuid_company, completeInfo.get("loComp"))
                for loCompHistory in loCompHistory:
                    loCompHistory.to_sql("lo_comp_histories", engine, if_exists="append", index=False, schema="company-management",dtype={ "broker_type": ARRAY(broker_type_enum), "company_id": Uuid()})

                # insert notes
                notes = map_external_notes(uuid_company, completeInfo.get("notes"))
                for note in notes:
                    note.to_sql('notes', engine, if_exists="append", index=False, schema="company-management")

                # insert tradeManagement
                tradeManagement = map_external_trade_management(uuid_company, completeInfo.get("tradeManagement"))
                tradeManagement.to_sql('trade_management', engine, if_exists="append", index=False, schema="company-management")

                # insert primarySalesRep
                jsonPrimarySalesRep = completeInfo.get("basicInfo").get("primarySalesRepAe")
                primarySalesRep = map_external_primary_sales_rep_ae(uuid_company, json.get("basicInfo").get("primarySalesRepAe"))
                primarySalesRep.to_sql("primary_sales_rep_ae", engine, if_exists="append", index=False, schema="company-management", dtype={"persona": ARRAY(persona_roles_enum)})

                # insert licenses
                license = map_external_license(uuid_company, completeInfo.get("license"))
                license.to_sql('licenses', engine, if_exists="append", index=False, schema="company-management")

                # insert license type
                df = map_external_license_types(license["id"].iloc[0], completeInfo.get("license").get("stateLicenseTypes", []))
                df.to_sql('license_types', engine, if_exists="append", index=False, schema="company-management")

                # insert onrp
                onrp = map_external_onrp(uuid_company, completeInfo.get("onrp"))
                onrp.to_sql('onrp', engine, if_exists="append", index=False, schema="company-management")

                # insert settings-onrp
                settingsOnrp = map_external_onrp_settings(onrp["id"].iloc[0], completeInfo.get("onrp"))
                settingsOnrp.to_sql('onrp_settings', engine, if_exists="append", index=False, schema="company-management")

                # insert custom fields
                customFields = map_external_custom_fields(uuid_company, completeInfo.get("customFields").get("fields", []))
                customFields.to_sql('custom_fields', engine, if_exists="append", index=False, schema="company-management")

                loanCriteria = map_external_loan_criteria(uuid_company, completeInfo.get("loanCriteria"))
                loanCriteria.to_sql('loan_criterias', engine, if_exists="append", index=False, schema="company-management")

                brokerLoanCriteria = map_external_broker(loanCriteria["id"].iloc[0], completeInfo.get("loanCriteria").get("broker", []))
                brokerLoanCriteria.to_sql('brokers', engine, if_exists="append", index=False, schema="company-management",
                                          dtype={"loan_types": ARRAY(loan_types_enum), "company_id": Uuid(),
                                                 "loan_purposes": ARRAY(loan_purposes_enum)})

                correspondantsLoanCriteria = map_external_correspondent(loanCriteria["id"].iloc[0], completeInfo.get("loanCriteria").get("correspondent"))
                correspondantsLoanCriteria.to_sql('correspondents', engine, if_exists="append", index=False, schema="company-management")

                loanCorrespondants = map_external_correspondent_settings(uuid_company, completeInfo.get("loanCriteria").get("correspondent"))
                loanCorrespondants.to_sql('correspondets_settings', engine, if_exists="append", index=False, schema="company-management")

            except IntegrityError as e:
                if "duplicate key value violates unique constraint" in str(e):
                    new_name = generate_unique_name(row["name"], engine)
                    print(f"🟡 Advertencia: {row['name']} ya existe. Se cambiará a '{new_name}'.")

                    row["name"] = new_name
                    row_df = pd.DataFrame([row])

                    row_df.to_sql('companies', engine,schema="company-management", if_exists="append", index=False)

                else:
                    print(f"⚠️ Error en la fila {index}: {e}")
                continue

            except Exception as e:
                errors += 1
                print(f"⚠️ Error inesperado en la fila {index}: {e}")
                continue

        print("✅ Datos insertados correctamente")
        print(f"Total de errores: {errors}")

    except Exception as e:
        print("⚠️ Error al insertar datos:", e)