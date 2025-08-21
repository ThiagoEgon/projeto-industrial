
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp_hour
from "duckdb_pipeline"."main"."agg_sensores_hora"
where timestamp_hour is null



  
  
      
    ) dbt_internal_test