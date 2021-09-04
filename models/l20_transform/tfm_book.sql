--- Using snowflake
-- with combine as
-- (
--     select
--         cast('dbt_hold_dev.public.manual_book1' as varchar) _dbt_source_relation,
--         b1.*
--     from {{ref('manual_book1')}} b1

--     union all

--     select
--         cast('dbt_hold_dev.public.manual_book2' as varchar) _dbt_source_relation,
--         b2.*
--     from {{ref('manual_book2')}} b2
-- )

-- select * from combine

--- Using dbt_utils
{{
    dbt_utils.union_relations(
        relations = [ref('manual_book1'),ref('manual_book2')]
    )
}}
