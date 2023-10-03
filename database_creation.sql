CREATE DATABASE FinalProjectData249;
USE FinalProjectData249;

CREATE TABLE trainers(
    trainer_id INT PRIMARY KEY IDENTITY(1,1),
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

CREATE TABLE course(
    course_id INT PRIMARY KEY IDENTITY(1,1),
    trainer_id INT FOREIGN KEY REFERENCES trainers(trainer_id),
    course_name VARCHAR(20)
);

CREATE TABLE personal_info(
    talent_id INT PRIMARY KEY IDENTITY(1,1),
    first_name VARCHAR(30),
    middle_name VARCHAR(30),
    last_name VARCHAR(30),
    gender VARCHAR(2),
    dob DATE,
    email VARCHAR(50),
    course_id INT FOREIGN KEY REFERENCES course(course_id)
);


CREATE TABLE candidate_sparta_data_information(
    candidate_id INT PRIMARY KEY IDENTITY(1,1),
    geo_flex VARCHAR,
    financial_support_self VARCHAR,
    course_interest VARCHAR,
    result INT,
    psychometric_score INT,
    presentation_score INT, 
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id)
);


CREATE TABLE behaviours (
    behaviour_id INT PRIMARY KEY IDENTITY(1,1),
    behaviour VARCHAR(20)
);

CREATE TABLE behaviours_by_week(
    behaviour_id INT FOREIGN KEY REFERENCES behaviours(behaviour_id),
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
    week_num INT,
    score INT, 
);

CREATE TABLE recruiters(
    recruiter_id INT PRIMARY KEY IDENTITY(1,1),
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

CREATE TABLE recruited_by (
    recruiter_id INT FOREIGN KEY REFERENCES recruiters(recruiter_id),
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
    combined_data DATE
);

CREATE TABLE city (
    city_id INT PRIMARY KEY IDENTITY(1,1),
    city VARCHAR(20)
);

CREATE TABLE address_junction (
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
    house_number INT,
    street VARCHAR,
    postcode VARCHAR,
    city_id INT FOREIGN KEY REFERENCES city(city_id)
);

CREATE TABLE uni (
    uni_id INT PRIMARY KEY IDENTITY(1,1),
    uni_name VARCHAR,
)

CREATE TABLE degree (
    degree_id INT PRIMARY KEY IDENTITY(1,1),
    course_name VARCHAR
)

CREATE TABLE uni_junction (
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
    uni_id INT FOREIGN KEY REFERENCES uni(uni_id),
    degree_id INT FOREIGN KEY REFERENCES degree(degree_id),
    degree_grade INT,
)

CREATE TABLE strengths (
    strength_id INT PRIMARY KEY IDENTITY(1,1),
    strength VARCHAR(50),
);

CREATE TABLE weaknesses (
    weakness_id INT PRIMARY KEY IDENTITY(1,1),
    weakness VARCHAR(50),
);

CREATE TABLE strengths_junction (
    strength_id INT FOREIGN KEY REFERENCES strengths(strength_id),
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
);

CREATE TABLE weaknesses_junction (
    weakness_id INT FOREIGN KEY REFERENCES weaknesses(weakness_id),
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
);

CREATE TABLE tech_scores (
    tech_score_id INT PRIMARY KEY IDENTITY(1,1),
    technology_name VARCHAR(50),
);

CREATE TABLE tech_junction (
    tech_score_id INT FOREIGN KEY REFERENCES weaknesses(weakness_id),
    talent_id INT FOREIGN KEY REFERENCES personal_info(talent_id),
    score INT
);