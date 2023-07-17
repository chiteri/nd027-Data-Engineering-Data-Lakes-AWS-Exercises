select * 
from accelerometer_landing AS a 
inner join customer_trusted AS c ON a.user = c.email 
