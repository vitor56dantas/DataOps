USE Mbade;
CREATE TABLE cadastro (
    id integer not null auto_increment,
    gender varchar(200),
    name varchar(200),
    title varchar(200),
    first varchar(200),
    last varchar(200),
    city varchar(200),
    state varchar(200),
    country varchar(200),
    email varchar(200),
    created_date datetime not null,
    KEY (id)    
);
SET character_set_client = utf8;
SET character_set_connection = utf8;
SET character_set_results = utf8;
SET collation_connection = utf8_general_ci;
INSERT INTO cadatro (gender, name, title, first, last, city, state, country, email, created_date) 
VALUES (1, "female", "Ms", "Lea", "Madsen", "Vesterborg", "Sjalland", "Denmark", "lea.madsen@example.com", "2024-03-16");