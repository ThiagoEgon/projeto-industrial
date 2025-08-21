
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_evento
from "duckdb_pipeline"."main"."fato_eventos"
where id_evento is null



  
  
      
    ) dbt_internal_test