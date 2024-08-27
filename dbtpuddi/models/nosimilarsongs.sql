select * from {{ ref('spotify') }}
where isrc not in (select isrc from {{ ref('similarsongsdrop') }})