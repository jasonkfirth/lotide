BEGIN;
	ALTER TABLE site
		DROP CONSTRAINT site_logo_href_scheme;

	ALTER TABLE site
		DROP COLUMN site_logo;
COMMIT;
