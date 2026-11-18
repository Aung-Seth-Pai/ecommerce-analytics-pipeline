{% docs col__op %}
Change type of this version of the row: I = insert, U = update, D = delete.
{% enddocs %}

{% docs col__source_updated_at %}
When this version of the row was written in the source.
{% enddocs %}

{% docs col__seq %}
Order of the row within its table in its batch.
{% enddocs %}

{% docs col__batch_id %}
The batch this row was delivered in. Added by the loader.
{% enddocs %}

{% docs col__batch_date %}
The day the batch was delivered: the UTC date of its window end. Added by the loader.
{% enddocs %}

{% docs col__loaded_at %}
When the loader appended this row's batch. Added by the loader.
{% enddocs %}