/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. Concatenate the timestamp and entities from the feature set table with the entity dataset.
 Feature values are joined to this table later for improved efficiency.
 featureset_timestamp is equal to null in rows from the entity dataset.
 */
WITH union_features AS (
SELECT
  -- uuid is a unique identifier for each row in the entity dataset. Generated by `QueryTemplater.createEntityTableUUIDQuery`
  row_number,
  -- event_timestamp contains the timestamps to join onto
  event_timestamp,
  -- the feature_timestamp, i.e. the latest occurrence of the requested feature relative to the entity_dataset timestamp
  NULL as {{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp,
  -- created timestamp of the feature at the corresponding feature_timestamp
  NULL as created_timestamp,
  -- select only entities belonging to this feature set
  {{ featureSet.entities | join(', ')}},
  -- boolean for filtering the dataset later
  true AS is_entity_table
FROM {{ leftTableName }}

UNION ALL
SELECT
  NULL as row_number,
  event_timestamp,
  event_timestamp as {{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp,
  created_timestamp,
  {% for entity in featureSet.entities %}
  {{variantColumn}}:{{entity}} as {{entity}},
  {% endfor %}
  false AS is_entity_table
FROM {{feastTable}} WHERE project = '{{ featureSet.project}}' AND FEATURESET = '{{featureSet.name}}' AND event_timestamp <= '{{maxTimestamp}}'
{% if featureSet.maxAge == 0 %}{% else %}AND event_timestamp >= dateadd(second,-{{featureSet.maxAge}},'{{minTimestamp}}'){% endif %}
),
/*
 2. Window the data in the unioned dataset, partitioning by entity and ordering by event_timestamp, as
 well as is_entity_table.
 Within each window, back-fill the feature_timestamp - as a result of this, the null feature_timestamps
 in the rows from the entity table should now contain the latest timestamps relative to the row's
 event_timestamp.

 For rows where event_timestamp(provided datetime) - feature_timestamp > max age, set the
 feature_timestamp to null.
 */
joined AS (
SELECT
  row_number,
  event_timestamp,
  {{ featureSet.entities | join(', ')}},
  {% for feature in featureSet.features %}
  CASE WHEN event_timestamp >= {{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp THEN {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }} ELSE NULL END as {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM (
SELECT
  row_number,
  event_timestamp,
  {{ featureSet.entities | join(', ')}},
  FIRST_VALUE(created_timestamp) over(PARTITION BY {{ featureSet.entities | join(', ') }} ORDER BY event_timestamp DESC, is_entity_table DESC, created_timestamp DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS created_timestamp,
  FIRST_VALUE({{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp) over(PARTITION BY {{ featureSet.entities | join(', ') }} ORDER BY event_timestamp DESC, is_entity_table DESC, created_timestamp DESC ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS {{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp,
  is_entity_table
FROM union_features
) as uf
/*
 3. Select only the rows from the entity table, and join the features from the original feature set table
 to the dataset using the entity values, feature_timestamp, and created_timestamps.
 */
LEFT JOIN (
SELECT
  event_timestamp as {{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp,
  created_timestamp,
  {% for entity in featureSet.entities %}
  {{variantColumn}}:{{entity}} as {{entity}},
  {% endfor %}
  {% for feature in featureSet.features %}
  {{variantColumn}}:{{feature.name }} as {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM {{feastTable}} WHERE project = '{{ featureSet.project}}' AND FEATURESET = '{{featureSet.name}}' AND event_timestamp <= '{{maxTimestamp}}'
{% if featureSet.maxAge == 0 %}{% else %}AND event_timestamp >= dateadd(second,-{{featureSet.maxAge}},'{{minTimestamp}}'){% endif %}
) as l_{{ featureSet.project }}_{{ featureSet.name }} USING ({{ featureSet.project }}_{{ featureSet.name }}_feature_timestamp, created_timestamp, {{ featureSet.entities | join(', ')}})
WHERE is_entity_table
)
/*
 4. Finally, deduplicate the rows by selecting the first occurrence of each entity table row row_number.
 */
SELECT
    row_number,
    event_timestamp,
    {{ featureSet.entities | join(', ')}},
    {% for feature in featureSet.features %}
    {{ featureSet.project }}__{{ featureSet.name }}__{{ feature.name }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
FROM (
         SELECT j.*, ROW_NUMBER() OVER (PARTITION BY j.row_number ORDER BY event_timestamp DESC) AS rk
         FROM joined j
     ) s
WHERE s.rk = 1