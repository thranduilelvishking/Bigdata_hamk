
# **BigData_HAMK**

This project analyzes a Twitter dataset to determine whether the time frame of posting significantly impacts engagement levels. The dataset can be downloaded from Kaggle:  
(https://www.kaggle.com/datasets/goyaladi/twitter-dataset)

## **Requirements**

To run this project, ensure you have the following libraries installed:

- `numpy`
- `matplotlib`
- `pandas`
- `seaborn`
- `pymongo`
- `pyspark`
- `scipy`

### **Installation**
You can install these dependencies using:

```bash
pip install numpy matplotlib pandas seaborn pymongo pyspark scipy
```

Additionally, you need the following software:

- **MongoDB Compass** (for managing MongoDB)
- **Spark Mongo Connector** (for integrating Spark with MongoDB)

---

## **Database Setup**
Before running the project, set up a MongoDB database:

1. **Create a new database in MongoDB**  
   - **Recommended Settings:**
     - Database name: **`twitter_db`**
     - Collection name: **`tweets`**  
   Keeping these names will prevent the need for code modifications.

2. **Download and place required `.jar` files**  
   The following `.jar` files must be placed in the `jars` folder within your Spark installation:

   - [`mongo-java-driver-3.12.11.jar`](https://mvnrepository.com/artifact/org.mongodb/mongo-java-driver/3.12.11)
   - [`mongo-spark-connector_2.12-3.0.1.jar`](https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector_2.12/3.0.1)

---

## **Additional Requirements**
- Ensure **Java** is installed on your system, as Spark requires it.
