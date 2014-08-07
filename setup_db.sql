create database PubMedRepository;
use PubMedRepository;
create table if not exists article_meta(aid varchar(64), accession varchar(10),pmcid varchar(10),pmc_uid varchar(10),publisher_id varchar(50),pmid varchar(10),doi varchar(500),title_paper varchar(500),journal_id varchar(60),journal_title varchar(500),date_of_pub varchar(20),abstract longtext,authors varchar(8000),primary key (aid)) character set utf8 collate utf8_general_ci;
create table if not exists article_references(rid varchar(64),refid varchar(100),aid varchar(64),title varchar(2000),authors_list varchar(5000),pmid varchar(10),primary key(rid),foreign key (aid) references article_meta(aid)) character set utf8 collate utf8_general_ci;
create table if not exists currentdate(last_insert_date date) character set utf8 collate utf8_general_ci;
