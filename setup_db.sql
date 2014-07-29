create database PubMedRepository;
use PubMedRepository;
create table if not exists article_meta(aid varchar(64), accession varchar(10),pmcid varchar(10),pmc_uid varchar(10),publisher_id varchar(50),pmid varchar(10),doi varchar(10),title_paper varchar(500),journal_id varchar(60),journal_title varchar(500),date_of_pub varchar(10),abstract longtext,authors varchar(500),primary key (aid));
create table if not exists article_authors(aid varchar(64),last_name varchar(20),given_name varchar(20),foreign key (aid) references article_meta(aid));
create table if not exists article_references(rid varchar(64),refid varchar(64),aid varchar(64),title varchar(500),authors_list varchar(1000),pmid varchar(10),primary key(rid),foreign key (aid) references article_meta(aid));
create table if not exists currentdate(last_insert_date date);
