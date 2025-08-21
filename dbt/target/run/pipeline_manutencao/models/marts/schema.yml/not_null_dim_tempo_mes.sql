
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mes
from "duckdb_pipeline"."main"."dim_tempo"
where mes is null



  
  
      
    ) dbt_internal_test