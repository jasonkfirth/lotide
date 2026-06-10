/*
    Project: Lotide database migrations
    -----------------------------------

    File: migrations/20260609002000_site-css/down.sql

    Purpose:

        Remove the optional site stylesheet setting.

    Responsibilities:

        - drop the local stylesheet URL constraint
        - drop the site_css column

    This file intentionally does NOT contain:

        - media row cleanup
        - frontend rendering changes
*/

BEGIN;
    ALTER TABLE site
        DROP CONSTRAINT site_css_href_scheme;

    ALTER TABLE site
        DROP COLUMN site_css;
COMMIT;

/* end of migrations/20260609002000_site-css/down.sql */
