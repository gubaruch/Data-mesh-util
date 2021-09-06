# Data Mesh Producer Library

The `DataMeshProducer.py` library provides functions to assist data __Producers__ to create and manage __Data Products__. The following methods are avialable:

* [`create_data_products`](#create_data_products)
* [`list_pending_access_requests`](#list_pending_access_requests)
* [`approve_access_request`](#approve_access_request)
* [`deny_access_request`](#deny_access_request)
* [`update_subscription_permissions`](#update_subscription)
* [`delete_subscription`](#delete_subscription)

## Method Detail

### create\_data\_products

Creates a new data product offering of one-or-more tables. When creating a set of data products, the object metadata is copied into the Lake Formation catalog of the data mesh account, and appropriate grants are created to enable the product to administer the central metadata.

#### Request Syntax

```
create_data_products(
	data_mesh_account_id: str, 
	source_database_name: str,
	table_name_regex: str = None, 
	sync_mesh_catalog_schedule: str = None,
	sync_mesh_crawler_role_arn: str = None
)
```

#### Parameters

* `data_mesh_account_id` (String) - The AWS Account ID of the Account used as the central data mesh repository
* `source_database_name` (String) - The name of the Source Database. Only 1 Database at a time may be used to create a set of data products
* `table_name_regex` (String) - A table name or regular expression matching a set of tables to be offered. Optional.
* `sync_mesh_catalog_schedule` (String) - CRON expression indicating how often the data mesh catalog should be synced with the source. Optional. If not provided, metadata will be updated every 4 hours if a `sync_mesh_crawler_role_arn` is provided.
* `sync_mesh_crawler_role_arn` (String) - IAM Role ARN to be used to create a Glue Crawler which will update the structure of the data mesh metadata based upon changes to the source. Optional. If not provided, metadata will not be updated from source.

#### Return Type

None

#### Response Structure

---

### list\_pending\_access\_requests

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### approve\_access\_request

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### deny\_access\_request

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### update\_subscription

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure

---

### delete\_subscription

#### Request Syntax

#### Parameters

#### Return Type

#### Response Structure
