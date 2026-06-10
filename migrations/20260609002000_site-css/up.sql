/*
    Project: Lotide database migrations
    -----------------------------------

    File: migrations/20260609002000_site-css/up.sql

    Purpose:

        Add an optional site stylesheet setting.

    Responsibilities:

        - store an uploaded local CSS file reference for the active site
        - keep the default stylesheet path implicit when the setting is NULL
        - reject non-local stylesheet references

    This file intentionally does NOT contain:

        - stylesheet upload logic
        - frontend rendering changes
*/

BEGIN;
    ALTER TABLE site
        ADD COLUMN site_css TEXT;

    ALTER TABLE site
        ADD CONSTRAINT site_css_href_scheme CHECK (
            site_css IS NULL
            OR site_css LIKE 'local-media://%'
        );
COMMIT;

/* end of migrations/20260609002000_site-css/up.sql */
