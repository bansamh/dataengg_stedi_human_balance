# Project: STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue allow us to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer for the STEDI team you are expected to build a data lakehouse solution for sensor data that trains a machine learning model.

## Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Implementation

### Landing Zone

**Glue Tables**:

- [customer_landing.sql](script/customer_landing.sql)
- [accelerometer_landing.sql](script/accelerometer_landing.sql)

**Athena**:
Landing Zone data query results

*Customer Landing*:

![customer_landing1](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/c353e772-6926-4905-8821-71bcd2faa2ef)
![customer_landing2](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/f4061890-9c1c-46c2-95c4-fcc33dcfe510)

*Accelerometer Landing*:

![accelerometer_landing1](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/5b7a71f3-8c4c-4ccb-a89c-1b134266ef5e)
![accelerometer_landing2](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/572a81f7-03c9-49a6-9385-a79199e3d18f)

### Trusted Zone

**Glue job scripts**:

- [customer_landing_to_trusted.py](scripts/customer_landing_to_trusted.py)
- [accelerometer_landing_to_trusted_zone.py](scripts/accelerometer_landing_to_trusted.py)

**Athena**:
Trusted Zone Query results:

![customer_trusted_count](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/17ffcb39-66b6-440b-b913-3e91d9fb424c)
![customer_trusted1](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/9199648c-2059-488b-8472-deafb51bf229)
![customer_trusted2](https://github.com/bansamh/dataengg_stedi_human_balance/assets/110283602/d903cb3d-de08-485a-b00e-deb672bdbe01)


### Curated Zone

**Glue job scripts**:

- [customer_trusted_to_curated.py](scripts/customer_trusted_to_curated.py)
- [step_trainer_trusted_to_curated.py](scripts/step_trainer_trusted_to_curated.py)
