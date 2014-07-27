start transaction;
create database PubMedRepository;
use PubMedRepository;
create table if not exists article_meta(aid int unsigned AUTO_INCREMENT, accession varchar(10),pmcid varchar(10),pmc_uid varchar(10),publisher_id varchar(50),pmid varchar(10),doi varchar(10),title varchar(500),journal varchar(500),date_of_pub DATE,abstract longtext,primary key (aid));
create table if not exists article_authors(aid int unsigned,last_name varchar(20),given_name varchar(20),foreign key (aid) references article_meta(aid));
create table if not exists article_references(refid varchar(10),aid int unsigned,title varchar(500),authors_list varchar(1000),primary key(refid),foreign key (aid) references article_meta(aid));
commit;
