import pandas as pd

from ksqldb_client.models.query_response import FinalMessageItem, Header, HeaderItem, QueryResponse, Row, RowItem
from ksqldb_client.testing import parse


def test_extract_columns_from_ksql_schema():
    schema = "`BOOKING_LOCATOR` STRING, `PNR_CREATE_DATE` TIMESTAMP, `TICKET_NUM` INTEGER, `COUPONS_INTERESTING_FIELDS` STRUCT<`COUPON_NUM` INTEGER, `BOARDPOINT_STN_CODE` STRING, `OFFPOINT_STN_CODE` STRING, `TICKET_SALE_DATE_TIME` TIMESTAMP>, `NEW` STRUCT<`COUPON_NUM` INTEGER>"  # noqa: E501

    result = parse.extract_columns_from_ksql_schema(schema)

    expected_result = {
        "BOOKING_LOCATOR": "STRING",
        "PNR_CREATE_DATE": "TIMESTAMP",
        "TICKET_NUM": "INTEGER",
        "COUPONS_INTERESTING_FIELDS": "STRUCT",
        "NEW": "STRUCT",
    }

    assert result == expected_result


def test_dictionary_keys_to_lowercase():
    dict_ = {
        "Key1": "Value1",
        "Key2": {
            "SubKey1": "SubValue1",
            "SubKey2": {
                "SubSubKey1": "SubSubValue1",
            },
        },
        "Key3": (
            {
                "ListKey1": "ListValue1",
            },
            "ListValue2",
        ),
    }

    result = parse.dictionary_keys_to_lowercase_recursively(dict_)

    expected_result = {
        "key1": "Value1",
        "key2": {
            "subkey1": "SubValue1",
            "subkey2": {
                "subsubkey1": "SubSubValue1",
            },
        },
        "key3": (
            {
                "listkey1": "ListValue1",
            },
            "ListValue2",
        ),
    }

    assert result == expected_result


def test_query_response_to_pandas() -> None:
    test_input = QueryResponse(
        root=(
            HeaderItem(
                header=Header(
                    query_id="transient_COMMERCIAL_REVMAN_ALERTS_TICKETING_AVRO_0_3222959184482351391",
                    schema_="`MP5_ROWKEY` STRUCT<`HAUL` STRING, `CITY_ORIGIN` STRING, `CITY_DESTINATION` STRING, `RBD` STRING>, `BOARDPOINT_STN_CODE` STRING, `OFFPOINT_STN_CODE` STRING, `BOARDPOINT_CITY_CODE` STRING, `OFFPOINT_CITY_CODE` STRING, `BOARDPOINT_COUNTRY_CODE` STRING, `OFFPOINT_COUNTRY_CODE` STRING, `HAUL` STRING, `TICKETING_CARRIER` STRING, `TICKET_NUM` BIGINT, `COUPON` STRUCT<`COUPON_NUM` INTEGER, `BOARDPOINT_STN_CODE` STRING, `OFFPOINT_STN_CODE` STRING, `DEPARTURE_DATETIME` TIMESTAMP, `OP_CARRIER_CODE` STRING, `MKTG_CARRIER_CODE` STRING, `FARE_BASIS_CODE` STRING, `SOLD_CLASS_CODE` STRING, `SOLD_CABIN_CODE` STRING, `COUPON_USAGE_CODE` STRING, `BOOKING_STATUS` STRING, `SEGMENT_ID` STRING, `IMMEDIATE_DEPARTURE` BOOLEAN, `TICKET_DESIGNATOR` STRING>, `BOOKING_LOCATOR` STRING, `BOOKING_CREATE_DATE` DATE, `TICKET_SALE_DATE_TIME` TIMESTAMP, `PRIME_TICKET_NUM` BIGINT, `DISCOUNTS` STRUCT<`F1_DISCOUNT` DOUBLE, `F2_DISCOUNT` DOUBLE, `RC_DISCOUNT` DOUBLE, `RM_DISCOUNT` DOUBLE, `BP_DISCOUNT` DOUBLE, `BI_DISCOUNT` DOUBLE, `RE_DISCOUNT` DOUBLE>, `TICKET_HISTORY` ARRAY<STRING>, `IS_SPECIAL_CLASS` BOOLEAN, `IS_FARE_BASIS_FF` BOOLEAN, `LINERATE_FARE_CALCULATION_AREA` STRING, `FARE` STRUCT<`TICKET_FARE__GROSS_REVENUE` DOUBLE, `FARE_FREQUENCY__GROSS_REVENUE_CURRENCY` STRING, `TOTAL_AMOUNT__TOTAL_PAYMENT_ORIG` DOUBLE, `EQUIVALENT_AMOUNT__GROSS_REVENUE_ORIG` DOUBLE, `EQUIVALENT_CURRENCY__SALE_CURRENCY_CODE_ORIG` STRING, `IS_FARE_CALCULATION_MANUAL` BOOLEAN, `TOTAL_AMOUNT_DISCOUNTS_REVERTED` DOUBLE>, `TAXES` ARRAY<STRUCT<`TAX_CODE` STRING, `TAX_AMOUNT` DOUBLE, `IS_PAID` BOOLEAN>>, `MIN_PRICE` DOUBLE, `DAYS_SINCE_TICKET_SALE` BIGINT, `H_SINCE_TICKET_SALE` BIGINT, `MIN_SINCE_TICKET_SALE` BIGINT, `S_SINCE_TICKET_SALE` BIGINT",  # noqa: E501
                ),
            ),
            RowItem(
                row=Row(
                    columns=(
                        {"HAUL": "MH", "CITY_ORIGIN": "AMS", "CITY_DESTINATION": "MAD", "RBD": "M"},
                        "AMS",
                        "MAD",
                        "AMS",
                        "MAD",
                        "NL",
                        "ES",
                        "MH",
                        "075",
                        1428139709,
                        {
                            "COUPON_NUM": 1,
                            "BOARDPOINT_STN_CODE": "AMS",
                            "OFFPOINT_STN_CODE": "MAD",
                            "DEPARTURE_DATETIME": 1713355200000,
                            "OP_CARRIER_CODE": "I2",
                            "MKTG_CARRIER_CODE": "IB",
                            "FARE_BASIS_CODE": "QDNNENB2",
                            "SOLD_CLASS_CODE": "M",
                            "SOLD_CABIN_CODE": "Y",
                            "COUPON_USAGE_CODE": "OPEN FOR USE",
                            "BOOKING_STATUS": "OK",
                            "SEGMENT_ID": "AMSMAD17APR24T1200",
                            "IMMEDIATE_DEPARTURE": False,
                            "TICKET_DESIGNATOR": "",
                        },
                        "M1ESY",
                        "2024-04-16",
                        "2024-04-16T11:19:00.000",
                        1428139709,
                        {
                            "F1_DISCOUNT": 0.0,
                            "F2_DISCOUNT": 0.0,
                            "RC_DISCOUNT": 0.0,
                            "RM_DISCOUNT": 0.0,
                            "BP_DISCOUNT": 0.0,
                            "BI_DISCOUNT": 0.0,
                            "RE_DISCOUNT": 0.0,
                        },
                        [
                            "SERVIBERIA+ ES",
                            "1A/HDQIB0175-78496445/MADIB0001-78490370//P1",
                            "IB3723/17APR24/M/AMSMAD      CHECKED IN",
                            "IB3723/17APR24/M/AMSMAD      OPEN FOR USE",
                            "ADDED NEW EMD NUMBER 0754404305633C1",
                        ],
                        False,
                        False,
                        "I-15APR24MAD IB AMS51.13IB MAD51.13NUC102.26END ROE0.928954",
                        {
                            "TICKET_FARE__GROSS_REVENUE": 95.0,
                            "FARE_FREQUENCY__GROSS_REVENUE_CURRENCY": "EUR",
                            "TOTAL_AMOUNT__TOTAL_PAYMENT_ORIG": 0.0,
                            "EQUIVALENT_AMOUNT__GROSS_REVENUE_ORIG": None,
                            "EQUIVALENT_CURRENCY__SALE_CURRENCY_CODE_ORIG": None,
                            "IS_FARE_CALCULATION_MANUAL": False,
                            "TOTAL_AMOUNT_DISCOUNTS_REVERTED": 0.0,
                        },
                        [
                            {"TAX_CODE": "QV", "TAX_AMOUNT": 3.54, "IS_PAID": False},
                            {"TAX_CODE": "OG", "TAX_AMOUNT": 0.63, "IS_PAID": False},
                            {"TAX_CODE": "JD", "TAX_AMOUNT": 14.62, "IS_PAID": False},
                            {"TAX_CODE": "CJ", "TAX_AMOUNT": 17.08, "IS_PAID": False},
                            {"TAX_CODE": "RN", "TAX_AMOUNT": 24.15, "IS_PAID": False},
                            {"TAX_CODE": "VV", "TAX_AMOUNT": 29.05, "IS_PAID": False},
                        ],
                        115.0,
                        0,
                        1,
                        4,
                        37,
                    ),
                    tombstone=None,
                ),
            ),
            FinalMessageItem(final_message="Limit Reached"),
        ),
    )

    test_expected_output = pd.DataFrame(
        [
            {
                "mp5_rowkey": {"haul": "MH", "city_origin": "AMS", "city_destination": "MAD", "rbd": "M"},
                "boardpoint_stn_code": "AMS",
                "offpoint_stn_code": "MAD",
                "boardpoint_city_code": "AMS",
                "offpoint_city_code": "MAD",
                "boardpoint_country_code": "NL",
                "offpoint_country_code": "ES",
                "haul": "MH",
                "ticketing_carrier": "075",
                "ticket_num": 1428139709,
                "coupon": {
                    "coupon_num": 1,
                    "boardpoint_stn_code": "AMS",
                    "offpoint_stn_code": "MAD",
                    "departure_datetime": 1713355200000,
                    "op_carrier_code": "I2",
                    "mktg_carrier_code": "IB",
                    "fare_basis_code": "QDNNENB2",
                    "sold_class_code": "M",
                    "sold_cabin_code": "Y",
                    "coupon_usage_code": "OPEN FOR USE",
                    "booking_status": "OK",
                    "segment_id": "AMSMAD17APR24T1200",
                    "immediate_departure": False,
                    "ticket_designator": "",
                },
                "booking_locator": "M1ESY",
                "booking_create_date": pd.Timestamp("2024-04-16 00:00:00+0000", tz="UTC"),
                "ticket_sale_date_time": pd.Timestamp("2024-04-16 11:19:00+0000", tz="UTC"),
                "prime_ticket_num": 1428139709,
                "discounts": {
                    "f1_discount": 0.0,
                    "f2_discount": 0.0,
                    "rc_discount": 0.0,
                    "rm_discount": 0.0,
                    "bp_discount": 0.0,
                    "bi_discount": 0.0,
                    "re_discount": 0.0,
                },
                "ticket_history": [
                    "SERVIBERIA+ ES",
                    "1A/HDQIB0175-78496445/MADIB0001-78490370//P1",
                    "IB3723/17APR24/M/AMSMAD      CHECKED IN",
                    "IB3723/17APR24/M/AMSMAD      OPEN FOR USE",
                    "ADDED NEW EMD NUMBER 0754404305633C1",
                ],
                "is_special_class": False,
                "is_fare_basis_ff": False,
                "linerate_fare_calculation_area": "I-15APR24MAD IB AMS51.13IB MAD51.13NUC102.26END ROE0.928954",
                "fare": {
                    "ticket_fare__gross_revenue": 95.0,
                    "fare_frequency__gross_revenue_currency": "EUR",
                    "total_amount__total_payment_orig": 0.0,
                    "equivalent_amount__gross_revenue_orig": None,
                    "equivalent_currency__sale_currency_code_orig": None,
                    "is_fare_calculation_manual": False,
                    "total_amount_discounts_reverted": 0.0,
                },
                "taxes": [
                    {"TAX_CODE": "QV", "TAX_AMOUNT": 3.54, "IS_PAID": False},
                    {"TAX_CODE": "OG", "TAX_AMOUNT": 0.63, "IS_PAID": False},
                    {"TAX_CODE": "JD", "TAX_AMOUNT": 14.62, "IS_PAID": False},
                    {"TAX_CODE": "CJ", "TAX_AMOUNT": 17.08, "IS_PAID": False},
                    {"TAX_CODE": "RN", "TAX_AMOUNT": 24.15, "IS_PAID": False},
                    {"TAX_CODE": "VV", "TAX_AMOUNT": 29.05, "IS_PAID": False},
                ],
                "min_price": 115.0,
                "days_since_ticket_sale": 0,
                "h_since_ticket_sale": 1,
                "min_since_ticket_sale": 4,
                "s_since_ticket_sale": 37,
            },
        ],
    ).astype(
        {
            "mp5_rowkey": "object",
            "boardpoint_stn_code": pd.StringDtype(),
            "offpoint_stn_code": pd.StringDtype(),
            "boardpoint_city_code": pd.StringDtype(),
            "offpoint_city_code": pd.StringDtype(),
            "boardpoint_country_code": pd.StringDtype(),
            "offpoint_country_code": pd.StringDtype(),
            "haul": pd.StringDtype(),
            "ticketing_carrier": pd.StringDtype(),
            "ticket_num": pd.Int64Dtype(),
            "coupon": "object",
            "booking_locator": pd.StringDtype(),
            "booking_create_date": pd.DatetimeTZDtype(tz="utc"),
            "ticket_sale_date_time": pd.DatetimeTZDtype(tz="utc"),
            "prime_ticket_num": pd.Int64Dtype(),
            "discounts": "object",
            "ticket_history": "object",
            "is_special_class": pd.BooleanDtype(),
            "is_fare_basis_ff": pd.BooleanDtype(),
            "linerate_fare_calculation_area": pd.StringDtype(),
            "fare": "object",
            "taxes": "object",
            "min_price": pd.Float64Dtype(),
            "days_since_ticket_sale": pd.Int64Dtype(),
            "h_since_ticket_sale": pd.Int64Dtype(),
            "min_since_ticket_sale": pd.Int64Dtype(),
            "s_since_ticket_sale": pd.Int64Dtype(),
        },
    )

    test_output = parse.query_response_to_pandas(test_input)

    pd.testing.assert_frame_equal(test_output, test_expected_output)
