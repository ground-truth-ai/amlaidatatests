-- Valid two-letter unicode format
SELECT
  COUNT(*) AS "CountStar()"
FROM (
  SELECT
    CAST(GET("t1"."residencies", 'region_code') AS VARCHAR) AS "field"
  FROM (
    SELECT
      "t0"."party_id",
      "t0"."validity_start_time",
      "t0"."is_entity_deleted",
      "t0"."source_system",
      "t0"."type",
      "t0"."birth_date",
      "t0"."establishment_date",
      "t0"."occupation",
      "t0"."gender",
      "t0"."nationalities",
      CAST(NULLIF(IFF(_u.pos = _u_2.pos_2, _u_2."residencies", NULL), '') AS OBJECT) AS "residencies",
      "t0"."exit_date",
      "t0"."join_date",
      "t0"."assets_value_range",
      "t0"."civil_status_code",
      "t0"."education_level_code"
    FROM "party" AS "t0"
    CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(
      0,
      (
        GREATEST(
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '706cd818426c43d0b4cb14703291d15b'),
              '706cd818426c43d0b4cb14703291d15b'
            )
          )
        ) - 1
      ) + 1
    ))) AS _u(seq, key, path, index, pos, this)
    CROSS JOIN TABLE(FLATTEN(INPUT => SPLIT(
      ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '706cd818426c43d0b4cb14703291d15b'),
      '706cd818426c43d0b4cb14703291d15b'
    ))) AS _u_2(seq, key, path, pos_2, "residencies", this)
    WHERE
      _u.pos = _u_2.pos_2
      OR (
        _u.pos > (
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '706cd818426c43d0b4cb14703291d15b'),
              '706cd818426c43d0b4cb14703291d15b'
            )
          ) - 1
        )
        AND _u_2.pos_2 = (
          ARRAY_SIZE(
            SPLIT(
              ARRAY_TO_STRING(NULLIF("t0"."residencies", []), '706cd818426c43d0b4cb14703291d15b'),
              '706cd818426c43d0b4cb14703291d15b'
            )
          ) - 1
        )
      )
  ) AS "t1"
  WHERE
    NOT (
      CAST(GET("t1"."residencies", 'region_code') AS VARCHAR) IN ('AC', 'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BL', 'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY', 'BZ', 'CA', 'CC', 'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN', 'CO', 'CP', 'CQ', 'CR', 'CU', 'CV', 'CW', 'CX', 'CY', 'CZ', 'DE', 'DG', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EA', 'EC', 'EE', 'EG', 'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM', 'FO', 'FR', 'GA', 'GB', 'GD', 'GE', 'GF', 'GG', 'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT', 'GU', 'GW', 'GY', 'HK', 'HM', 'HN', 'HR', 'HT', 'HU', 'IC', 'ID', 'IE', 'IL', 'IM', 'IN', 'IO', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH', 'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR', 'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM', 'PN', 'PR', 'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SX', 'SY', 'SZ', 'TA', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU', 'WF', 'WS', 'XK', 'YE', 'YT', 'ZA', 'ZM', 'ZW')
    )
) AS "t2"
