BEGIN;
	ALTER TABLE site
		ADD COLUMN site_logo TEXT;

	ALTER TABLE site
		ADD CONSTRAINT site_logo_href_scheme CHECK (
			site_logo IS NULL
			OR site_logo LIKE 'local-media://%'
			OR site_logo LIKE 'https://%'
			OR site_logo LIKE 'http://%'
		);
COMMIT;
