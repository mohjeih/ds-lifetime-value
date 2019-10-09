select
@colnames
from (

select
      memberID,
      Flag_Reseller as flagReseller
from  DS_RESELLER

) x