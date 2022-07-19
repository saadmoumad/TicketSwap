CREATE TABLE IF NOT EXISTS public.Comments (
	Id integer NOT NULL,
	PostId integer NOT NULL,
	Score integer NOT NULL,
	Text VARCHAR NOT NULL,
	CreationDate TIMESTAMP NOT NULL,
	UserId integer,
    ContentLicense VARCHAR(50),
	UserDisplayName VARCHAR(100),
	PRIMARY KEY (Id)
);
