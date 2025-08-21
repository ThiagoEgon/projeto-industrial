
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id_maquina
from "duckdb_pipeline"."main"."fato_operacional"
where id_maquina is null



  
  
      
    ) dbt_internal_test