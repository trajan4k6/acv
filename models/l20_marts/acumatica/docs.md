{% docs core_id %}

Core_id is a unique id determined by Account Classification.  
It is dervied from account_classification as per below:

| account_classification  |  core_id                    |
|-------------------------|-----------------------------|
| Core                    | invoice_no || product       |
| Enterprise              | invoice_no || asset_class   |


{% enddocs %}