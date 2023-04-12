# GCP Big Data ML Fundamentals

## I. Big Data and Machine Learning on Google Cloud
Google Cloud infrastructure has three layers:
  - Network and Security
  - Compute and Storage
  - Big Data and Machine Learning Products
### 1. Network and Security
- At the base layer is networking and security, which lays the foundation to support all of Google’s infrastructure and applications
### 2. Compute and Storage
- Compute: Google offers a range of Computing Services
  - Compute Engine: Compute Engine is an IaaS (Infrastructure as a Service) offering that provides computing, storage, and network virtually that are similar to physical data centers. Compute engine provides maximum flexibility for those who prefer to manage server instances themselves.
  - Google Kubernetes Engine (GKE): GKE runs containerized applications in a container that represents code packaged up with all its dependencies.
  - App Engine: A fully managed PaaS (platform as a service) offering. PaaS offers bind code to libraries that provide access to the infrastructure application needs. This allows more resources to be focused on application logic.
  - Cloud Functions: Executes code in response to events. This is often referred to as functions as a service.
  - Cloud Run: Cloud run is a fully managed to compute platform that enables you to run requests or event-driven stateless workloads without having to worry about servers. It abstracts away all infrastructure management, so you can focus on writing code. It automatically scales up and down from zero.
- Storage: Google Cloud offers both self-managed and fully managed database and storage services
  - Cloud Storage: Cloud Storage is suitable for unstructured data, while structured data has transactional and analytical workloads. Cloud Storage has four primary storage classes: Standard Storage, Nearline Storage, Coldline Storage, and Archive Storage, each with different use cases and costs.
  - Cloud Bigtable: Cloud Bigtable is suitable for analytical workloads that require only millisecond latency.
  - Cloud SQL: Cloud SQL is suitable for transactional workloads with SQL access.
  - Cloud Spanner: Cloud Spanner is suitable for transactional workloads with SQL access.
  - Firestore: Firestore is suitable for transactional workloads without SQL access.
  - BigQuery: BigQuery is best for analytical workloads with SQL access.
### 3. Big Data and Machine Learning Products
The big data and machine learning products are located on the top layer, allowing you to carry out operations such as gathering, storing, processing, and delivering business insights via data pipelines and ML models.
The products can be divided into four categories: ingestion and process, storage, analytics, and machine learning.
- Ingestion and process products are used for real-time and batch data and include Pub/Sub, Dataflow, and Cloud Data Fusion.
- The data storage category includes five products, including both relational and NoSQL databases.
- The major analytics tool is BigQuery, and there are also options for analyzing and visualizing data.
- The machine learning category includes the ML development platform, Vertex AI, and AI solutions for various markets.
## II. Data Engineering for Streaming Data
### 1. Big Data challenges
Data engineers face four major challenges known as the 4Vs: variety, volume, velocity, and veracity:
  - Variety refers to data coming from various sources in different formats. 
  - Volume is the challenge of handling a large amount of data that can vary from gigabytes to petabytes.
  - Velocity is the need to process data in near real-time and handle late, bad, or transformed data.
  - Veracity refers to data quality, as there may be inconsistencies and uncertainties.
### 2. Message-oriented architecture
- Google Cloud offers Pub/Sub, a tool for handling distributed message-oriented architectures at scale, which is a distributed messaging service that receives messages from a variety of device streams and ensures at least once delivery of received messages to subscribing applications with no provisioning required. 
- Pub/Sub's central element is a topic, which is like a radio antenna that can have zero, one, or more publishers and subscribers. Pub/Sub is a good solution to buffer changes for lightly coupled architectures that have many different publishers and subscribers. Finally, to get these messages reliably into a data warehouse, a pipeline that can match Pub/Sub scale and elasticity is needed.
### 3. Designing and Implementing streaming pipelines with Apache Beam
#### 3.1 Designing streaming pipelines with Apache Beam
- Dataflow can be used to create a pipeline that processes both streaming and batch data for analysis. This involves extracting, transforming, and loading data (ETL). When building a data pipeline, engineers need to consider whether the code will work for both batch and streaming data, if the SDK has the necessary tools, and if there are existing solutions or templates to use. 
- Apache Beam is a popular solution for pipeline design, providing a unified programming model for both batch and streaming data, and it is portable and extensible. It provides pipeline templates and supports Java, Python, and Go. The Apache Beam SDK includes various libraries for transformations and data connectors. Apache Beam creates a model representation from the code that can be executed on different engines, with Dataflow being a popular choice.
#### 3.2 Implementing streaming pipelines with Apache Beam
- Dataflow is designed to minimize maintenance overhead and automate tasks related to infrastructure setup, maintenance, monitoring, and scaling. This serverless computing execution model means that Google Cloud manages infrastructure tasks on behalf of users, including resource provisioning, performance tuning, and ensuring pipeline reliability. Dataflow also offers a wide variety of templates covering common use cases across Google Cloud products for both streaming and batch data. Users do not need to monitor the compute and storage resources that Dataflow manages to fit the demand of a streaming data pipeline.
### 4. Visualization with Looker and Looker Studio
#### 4.1 Looker
- Google Cloud provides Looker and Looker Studio to help stakeholders easily interact with and visualize data. Looker supports BigQuery and over 60 SQL databases. It allows developers to define a semantic modeling layer using LookML, which frees data engineers from interacting with individual databases to focus on business logic. Looker is 100% web-based and has multiple data visualization options, including dashboards, area charts, line charts, and Sankey diagrams. Looker dashboards can be shared with teams through storage services like Google Drive, Slack, and Dropbox. Looker also lets you plot data on a map to see ride distribution, busy areas, and peak hours. These features help to make data insights easy to understand and can help identify customer frustrations, uncover lost revenue, and make better business decisions.
#### 4.2 Looker Studio
- Looker Studio is another data visualization tool offered by Google that is integrated into BigQuery. Unlike Looker, Looker Studio doesn't require an administrator to establish a data connection, making data visualization possible with just a few clicks. Looker Studio dashboards are integrated into various Google products and applications, such as Google Analytics and Google Cloud billing. Creating a Looker Studio dashboard requires three steps: choosing a template, linking the dashboard to a data source, and exploring the dashboard.
## III. Big Data with BigQuery
### 1. Storage and analytics
- BigQuery is a data management and analytical tool provided by Google that consists of a fully-managed storage facility and an SQL-based analytical engine, connected by Google's high-speed internal network. It can ingest datasets from various sources such as internal, external, multi-Cloud, and public data-sets, and offers three basic patterns to load data: batch load, streaming, and generated data. 
- BigQuery is optimized for running analytical queries over large datasets and supports ad hoc analysis using standard SQL, geospatial analytics, machine learning, and building business intelligence dashboards. BigQuery offers both interactive and batch queries.
### 2. BigQuery ML
#### 2.1. Introduction to BigQuery ML
- BigQuery has evolved from being just a data warehouse to support the data to AI lifecycle. It provides features to build machine learning models in the ML project phases using SQL commands. Building and training ML models can be time-intensive, but with BigQuery ML, you can create and execute ML models on structured data sets using SQL queries. BigQuery ML is designed to be simple, and you can define the machine learning hyperparameters to tune the model for the best training result.
- BigQuery supports supervised and unsupervised models, and you need to choose the appropriate model type depending on your business goal and datasets. BigQuery ML also supports ML Ops, which includes deploying, monitoring, and managing ML production.
#### 2.2. BigQuery ML project phase
The key phases of a machine learning project:
- In Phase 1, data is extracted, transformed, and loaded into BigQuery. 
- In Phase 2, features are selected and preprocessed using SQL to create a training dataset for the model to learn from. 
- In Phase 3, the model is created inside BigQuery using a create model command and a sequel query with the training dataset. 
- In Phase 4, the model's performance is evaluated using an ML dot evaluate query to analyze lost metrics like root mean squared error and area under the curve accuracy. 
- In Phase 5, when the model's performance is satisfactory, it can be used to make predictions using the ML dot predict command, returning the model's predictions and confidence level.
## IV. Machine Learning Options on Google Cloud
### 1. Options to build ML models
Google Cloud provides four options to build machine learning models, including BigQuery ML, pre-built APIs, AutoML, and custom training:
- BigQuery ML is suitable for users who already have their data in BigQuery and want to use SQL queries to create and execute machine learning models.
- Pre-built APIs are ideal for users who lack training data or ML expertise and want to leverage pre-built models for common perceptual tasks such as vision, video, and natural language.
- AutoML is a no-code solution for users who want to build their machine learning models on Vertex AI through a point-and-click interface.
- Custom training is for users who want full control over their ML pipeline and have the ML expertise to code their own environment, training, and deployment.

The selection of the best option depends on various factors, such as business needs, data size, ML expertise, hyperparameter tuning, and time to train the model: 
- BigQuery ML supports tabular data only, while the other three support tabular image, text, and video data.
- Pre-built APIs don't require any training data, while BigQuery ML and custom training require a large amount of data.
- Pre-built APIs and AutoML are user-friendly with low requirements, while custom training has the highest requirement and BigQuery ML requires understanding SQL.
- Pre-built APIs do not allow tuning hyperparameters, while BigQuery ML and custom training do. 
The time to train a model depends on the project, and custom training takes the longest time as it builds the ML model from scratch.
### 2. Pre-built APIs
To create good machine learning models, you need high-quality training data, ideally hundreds of thousands of records. If you don't have that much data, pre-built APIs can be a great starting point. Pre-built APIs are services that act as building blocks for creating applications without the expense or complexity of building your own models. Google Cloud offers several pre-built APIs, including:
- The Speech-to-Text API
- The Cloud Natural Language API
- The Cloud Translation API
- The Text-to-Speech API
- The Vision API
- The Video Intelligence API

These APIs are already trained on Google datasets, such as YouTube captions for the Speech-to-Text API, and Google's image datasets for the Vision API. To try out the APIs, you can experiment with them in a browser, but when building a production model, you'll need to pass a JSON object request to the API and parse its response.
### 3. Auto ML
AutoML is an abbreviation for automated machine learning. AutoML automates machine learning pipelines and saves data scientists time on manual work, such as tuning hyperparameters and comparing multiple models. AutoML leverages two vital technologies:
- Transfer learning builds a knowledge base in the field.
- Neural architecture search finds the optimal model for the relevant project.

AutoML supports four types of data: image, tabular, text, and video. For each data type, AutoML solves different types of problems. AutoML can train high-quality custom machine learning models with minimal effort and requires little machine learning expertise. One of the biggest benefits of AutoML is that it is a no-code solution.
### 4. Custom training
The option of custom training for building machine learning models using Google Cloud: This can be done using Vertex AI Workbench, which is a single development environment for the entire data science workflow. Before coding begins, you must decide on the environment for your ML training code. You can choose a pre-built container, which is like a fully furnished kitchen with all the necessary dependencies, or a custom container, which is an empty room where you define the tools needed to complete the job. The pre-built container is recommended if your ML training requires a platform like TensorFlow, Pytorch, Scikit-learn or XGboost and Python code to work with the platform.
### 5. Vertex AI
Developing machine learning models and putting them into production is challenging, especially when it involves handling large quantities of data and getting models into production. Google's solution is Vertex AI, a unified platform that brings all the components of the machine learning ecosystem together, enabling users to create, deploy and manage models at scale. Users can upload data from various sources, create features, experiment with different models, and automatically monitor and improve production. Vertex AI offers AutoML and custom training options, and provides a smooth user experience, scalable ML ops, sustainability, and speedy model production.
## V. The Machine Learning Workflow with Vertex AI
### 1. Introduction
Vertex AI provides developers and data scientists with a unified environment to build custom ML models. The process is similar to serving food in a restaurant, starting with preparing raw ingredients through to serving dishes to a table. In traditional programming, data plus rules lead to answers, and a computer can only follow algorithms set up by humans. Machine learning allows machines to learn from data and examples to solve problems on their own. Data preparation involves data uploading and feature engineering, and model training requires iterative training to learn from the data. Model serving involves deploying, monitoring, and managing the model in production. The machine learning workflow is iterative and can be automated with ML Ops. Vertex AI provides two options for building ML models: AutoML and custom training. It also provides many features to support the workflow, including feature store, vizier, explainable AI, and pipelines.
### 2. Data preparation
In an AutoML workflow, the first stage is data preparation which involves uploading data and preparing it for model training through feature engineering. AutoML accepts four types of data: image, tabular, text, and video. To select the correct data type and objective, one should start by checking data requirements and add labels to the data if necessary. Labels can be manually added or through Google's paid label service. Data can be uploaded from a local source, BigQuery, or Cloud Storage. After data is uploaded, the next step is feature engineering, which is like processing ingredients before cooking. A feature is an independent variable that contributes to the prediction. To help with feature engineering, Vertex AI has a function called Feature Store, which is a centralized repository to organize, store, and serve machine learning features. The benefits of Vertex AI Feature Store include shareable, reusable, scalable, and easy-to-use features.
### 3. Model training
To train the model, we need to first prepare our data, which is like preparing ingredients for cooking. This involves two steps: 
- Model training
- Model evaluation

AI is an umbrella term that includes everything related to computers mimicking human intelligence, while machine learning is a subset of AI that mainly refers to supervised and unsupervised learning. Supervised learning is task-driven and identifies a goal, while unsupervised learning is data-driven and identifies a pattern. There are two major types of supervised learning: 
- Classification 
- Regression 
 
There are three major types of unsupervised learning: 
- Clustering
- Association 
- Dimensionality reduction
 
Google Cloud offers four machine learning options, with AutoML and pre-built APIs not requiring the specification of a machine learning model. With BigQuery ML and custom training, hyperparameters need to be assigned to guide the machine learning process. AutoML automatically adjusts hyperparameters in the back end using a neural architect search to find the best fit model.

### 4. Model evaluation
When experimenting with a recipe, constant tasting is necessary to ensure it meets expectations, and in machine learning, this is known as model evaluation. Vertex AI offers evaluation metrics based on confusion matrices and feature importance. A confusion matrix is a table that shows combinations of predicted and actual values, and it is the foundation for other metrics like recall and precision. Recall measures how many positive cases were predicted correctly, while precision measures how many cases predicted as positive were actually positive. In Vertex AI, the precision and recall curve can be adjusted based on the problem being solved. Feature importance is another useful measurement displayed through a bar chart that shows how each feature contributes to a prediction. The longer the bar, the more important the feature. Vertex AI also offers Explainable AI, a set of tools and frameworks to help interpret predictions made by machine learning models.
### 5. Model deployment and monitoring
In the machine learning workflow, the final stage is model serving, which involves model deployment and model monitoring. Model deployment is like serving a meal to a customer, and model monitoring is like checking that the restaurant is operating efficiently. Model management is important throughout the workflow to manage the machine-learning infrastructure. MLOps combines machine-learning development with operations and applies similar principles from DevOps to machine-learning models. It aims to solve production challenges related to machine learning by building an integrated machine learning system and operating it in production. Practicing MLOps involves advocating for automation and monitoring at each step of the ML system construction. There are three options for deploying a machine-learning model: endpoint deployment, batch prediction deployment, and offline prediction deployment. Model monitoring is supported by a tool called Vertex AI Pipelines, which automates, monitors, and governs machine-learning systems by orchestrating the workflow in a serverless manner. With Vertex AI Workbench, users can define their own pipeline using prebuilt pipeline components.
