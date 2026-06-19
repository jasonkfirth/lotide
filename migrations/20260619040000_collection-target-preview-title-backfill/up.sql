--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Backfill readable titles for cached source preview items created before
--     unnamed actor-feed Notes learned how to derive a first-line title.
--
-- Responsibilities:
--
--     - strip simple HTML tags from cached preview content
--     - collapse whitespace into a compact display title
--     - leave genuinely empty preview items as [no title]
--
-- This file intentionally does NOT contain:
--
--     - source preview refetching
--     - ActivityPub object parsing
--     - user interface rendering
--

BEGIN;
    DO $$
    BEGIN
        /*
            SQL_ASCII databases can contain UTF-8 bytes, but string functions
            count bytes there. Truncating a title in SQL could split a
            multibyte character, so SQL_ASCII installs should let normal
            preview refetching repair titles through Rust instead.
        */
        IF current_setting('server_encoding') = 'UTF8' THEN
            WITH derived_titles AS (
                SELECT
                    id,
                    LEFT(
                        TRIM(
                            regexp_replace(
                                regexp_replace(
                                    COALESCE(content_html, summary_html, ''),
                                    '<[^>]*>',
                                    ' ',
                                    'g'
                                ),
                                '[[:space:]]+',
                                ' ',
                                'g'
                            )
                        ),
                        80
                    ) AS new_name
                FROM collection_target_item
                WHERE name='[no title]'
            )
            UPDATE collection_target_item
            SET name=derived_titles.new_name
            FROM derived_titles
            WHERE collection_target_item.id=derived_titles.id
            AND derived_titles.new_name<>'';
        END IF;
    END $$;
COMMIT;

/* end of up.sql */
