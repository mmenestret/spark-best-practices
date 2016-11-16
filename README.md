# Spark best practices workshop 

The idea is to discuss about Spark applications best practices and in particular

1) How to organize and group the code (check the: `CodeOrganisationMain` main class)

    - Should it be organized based on technical constraints
    
    - Should it be organized based on the functionnal logic
    
2) Which API use (check the: `APIsOrUDFsMain` main class and the `APIsOrUDFsMainSpec` associated test class)

    - Dataframes
    
        - To leverage catalyst optimization
        
        - To profit from their flexibility
        
    - RDD
    
        - Use the type system (RDD[class1] => RDD[class2] is a lot better than Dataframe => Dataframe)
        
        - Easier to troubleshoot and optimize when mastered
        
3) How to transform the data (check the: `DataFrameOrRDDMain` main class)

    - Use the API functions
    
        - More optimized
        
        - Less serialization
        
    - Inject business functions into udf to decouple business code from Spark
    
        - Easier to test
        
        - Cleaner code
        
4) How to effectively log the dataflow and the operations on it
