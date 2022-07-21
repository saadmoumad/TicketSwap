CREATE TABLE IF NOT EXISTS public.Posts (
	Id integer NOT NULL,
	PostTypeId integer NOT NULL,
	AcceptedAnswerId integer,
	CreationDate TIMESTAMP NOT NULL,
    Score integer,
    ViewCount integer,
    Body VARCHAR NOT NULL,
    OwnerUserId integer,
    LastEditorUserId integer,
    LastEditDate TIMESTAMP,
    LastActivityDate TIMESTAMP,
    Title VARCHAR,
    Tags VARCHAR,
    AnswerCount integer,
    CommentCount integer,
    FavoriteCount integer,
    ContentLicense VARCHAR(50),
	PRIMARY KEY (Id)
);

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

CREATE TABLE IF NOT EXISTS public.Users (
	Id integer NOT NULL,
	Reputation integer NOT NULL,
	CreationDate TIMESTAMP NOT NULL,
    DisplayName VARCHAR,
    LastAccessDate TIMESTAMP not null,
    Location VARCHAR(50),
    AboutMe VARCHAR,
    Views integer,
    UpVotes integer,
    DownVotes integer,
    AccountId integer,
    ProfileImageUrl VARCHAR,
    WebsiteUrl VARCHAR,
	PRIMARY KEY (Id)
);

CREATE TABLE IF NOT EXISTS public.Badges (
	Id integer NOT NULL,
	UserId integer,
    Name VARCHAR(50),
	Date TIMESTAMP not null,
    Class integer,
    TagBased boolean not null,
	PRIMARY KEY (Id)
);

CREATE TABLE IF NOT EXISTS public.Tages (
	Id integer NOT NULL,
    TagName VARCHAR(50),
    Count integer,
    ExcerptPostId integer,
    WikiPostId integer,
	PRIMARY KEY (Id)
);

CREATE TABLE IF NOT EXISTS public.Votes (
	Id integer NOT NULL,
    PostId integer,
    VoteTypeId integer,
    CreationDate TIMESTAMP not null,
    UserId float,
    BountyAmount float,
	PRIMARY KEY (Id)
);

CREATE TABLE IF NOT EXISTS public.PostHistory (
	Id integer NOT NULL,
    PostHistoryTypeId integer,
    PostId integer,
    RevisionGUID VARCHAR(100),
    CreationDate TIMESTAMP not null,
    UserId float,
    Text VARCHAR,
    ContentLicense VARCHAR(50),
    Comment VARCHAR,
    UserDisplayName VARCHAR,
	PRIMARY KEY (Id)
);

CREATE TABLE IF NOT EXISTS public.PostLinks (
	Id integer NOT NULL,
    CreationDate TIMESTAMP not null,
    PostId integer,
    RelatedPostId integer,
    LinkTypeId integer,
	PRIMARY KEY (Id)
);


