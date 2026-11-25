-- T-SQL for the lh_ecosystem SQL analytics endpoint.
-- Run these after the Phase 1 pipeline has loaded Gold.
-- This is not Spark SQL: use TOP, not LIMIT.
-- Spark booleans usually arrive here as bit. Comparisons use 1 and 0.
-- The database context is the lakehouse endpoint, so two-part names are enough.
-- A repository in two technologies is counted in each of those technologies.
-- Sums across technologies are therefore not additive.

-- Twenty tracked repositories with the largest star change on any snapshot.
SELECT TOP (20)
    r.full_name,
    f.star_delta,
    f.stargazers_count,
    f.observed_at
FROM gold.fact_repository_snapshot AS f
INNER JOIN gold.dim_repository AS r
    ON f.github_repo_id = r.github_repo_id
WHERE f.star_delta IS NOT NULL
    AND f.snapshot_status <> 'missing'
ORDER BY f.star_delta DESC;

-- Newly discovered repositories by technology and week.
-- Week start follows the endpoint's DATEDIFF(WEEK) boundary.
SELECT
    t.technology_name,
    DATEADD(WEEK, DATEDIFF(WEEK, CAST('19000101' AS date), f.discovered_at), CAST('19000101' AS date)) AS week_start,
    COUNT(DISTINCT f.github_repo_id) AS repositories
FROM gold.fact_discovery AS f
INNER JOIN gold.dim_technology AS t
    ON f.technology_id = t.technology_id
GROUP BY
    t.technology_name,
    DATEADD(WEEK, DATEDIFF(WEEK, CAST('19000101' AS date), f.discovered_at), CAST('19000101' AS date))
ORDER BY week_start, t.technology_name;

-- Latest stars held by each technology.
-- A repository discovered under two technologies contributes to both totals.
WITH membership AS (
    SELECT DISTINCT github_repo_id, technology_id
    FROM gold.fact_discovery
),
latest_snapshot AS (
    SELECT github_repo_id, stargazers_count
    FROM (
        SELECT
            github_repo_id,
            stargazers_count,
            ROW_NUMBER() OVER (
                PARTITION BY github_repo_id
                ORDER BY observed_at DESC, collection_run_id DESC
            ) AS recency
        FROM gold.fact_repository_snapshot
        WHERE snapshot_status <> 'missing'
    ) AS ranked
    WHERE recency = 1
)
SELECT
    t.technology_name,
    COUNT(*) AS repositories,
    SUM(latest_snapshot.stargazers_count) AS stars
FROM membership
INNER JOIN latest_snapshot
    ON membership.github_repo_id = latest_snapshot.github_repo_id
INNER JOIN gold.dim_technology AS t
    ON membership.technology_id = t.technology_id
GROUP BY t.technology_name
ORDER BY stars DESC;

-- Current language mix for tracked repositories.
SELECT
    COALESCE(primary_language, 'Unknown') AS primary_language,
    COUNT(*) AS repositories
FROM gold.dim_repository
WHERE tracking_status = 'tracked'
GROUP BY primary_language
ORDER BY repositories DESC;

-- Topics on the latest observation date.
-- topics_json is a JSON array string. Repositories with no topics are absent here.
WITH latest_day AS (
    SELECT MAX(CAST(observed_at AS date)) AS observed_date
    FROM gold.fact_repository_snapshot
    WHERE snapshot_status <> 'missing'
)
SELECT TOP (20)
    topic.value AS topic,
    COUNT(DISTINCT f.github_repo_id) AS repositories
FROM gold.fact_repository_snapshot AS f
CROSS APPLY OPENJSON(f.topics_json) AS topic
INNER JOIN latest_day
    ON CAST(f.observed_at AS date) = latest_day.observed_date
WHERE f.snapshot_status <> 'missing'
GROUP BY topic.value
ORDER BY repositories DESC;

-- Archived and fork share among tracked repositories.
SELECT
    SUM(CASE WHEN is_archived = 1 THEN 1 ELSE 0 END) AS archived_repositories,
    SUM(CASE WHEN is_fork = 1 THEN 1 ELSE 0 END) AS fork_repositories,
    COUNT(*) AS repositories
FROM gold.dim_repository
WHERE tracking_status = 'tracked';

-- Discovered repositories that did not fit in the tracking cap.
SELECT COUNT(*) AS discovered_only_repositories
FROM gold.dim_repository
WHERE tracking_status = 'discovered_only';
