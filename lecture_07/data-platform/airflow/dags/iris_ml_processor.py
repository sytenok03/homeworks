import pandas as pd
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import os

def process_iris_data():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('ANALYTICS_DB', 'analytics'),
        user=os.getenv('ETL_USER', 'etl_user'),
        password=os.getenv('ETL_PASSWORD', 'etl_password')
    )
    
    # Read data from iris_processed table
    query = 'SELECT * FROM homework.iris_processed'
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f'Loaded data: {len(df)} rows, {len(df.columns)} columns')
    
    # Prepare features: drop species and one-hot encoded columns
    X = df.drop(columns=['species', 'species_setosa', 'species_versicolor', 'species_virginica'])
    
    # Create target from species column
    y = df['species'].map({'setosa': 0, 'versicolor': 1, 'virginica': 2})
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # Train full model
    model_full = RandomForestClassifier(n_estimators=100, random_state=42)
    model_full.fit(X_train, y_train)
    full_accuracy = model_full.score(X_test, y_test)
    
    # Get feature importance
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model_full.feature_importances_
    }).sort_values('importance', ascending=False)
    
    top_features = feature_importance.head(5)['feature'].tolist()
    
    # Train model with top 5 features
    X_train_top = X_train[top_features]
    X_test_top = X_test[top_features]
    model_top = RandomForestClassifier(n_estimators=100, random_state=42)
    model_top.fit(X_train_top, y_train)
    top_accuracy = model_top.score(X_test_top, y_test)
    
    # Save results to database
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('ANALYTICS_DB', 'analytics'),
        user=os.getenv('ETL_USER', 'etl_user'),
        password=os.getenv('ETL_PASSWORD', 'etl_password')
    )
    cursor = conn.cursor()
    
    # Create schema if not exists
    cursor.execute('CREATE SCHEMA IF NOT EXISTS ml_results')
    
    # Create metrics table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ml_results.iris_model_metrics (
            run_date TIMESTAMP,
            full_model_accuracy FLOAT,
            top5_model_accuracy FLOAT
        )
    ''')
    
    # Insert metrics
    cursor.execute('''
        INSERT INTO ml_results.iris_model_metrics VALUES (NOW(), %s, %s)
    ''', (full_accuracy, top_accuracy))
    
    # Create feature importance table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ml_results.iris_feature_importance (
            run_date TIMESTAMP,
            feature TEXT,
            importance FLOAT
        )
    ''')
    
    # Insert feature importance
    for _, row in feature_importance.iterrows():
        cursor.execute('''
            INSERT INTO ml_results.iris_feature_importance VALUES (NOW(), %s, %s)
        ''', (row['feature'], row['importance']))
    
    conn.commit()
    conn.close()
    
    return {
        'top_features': top_features,
        'full_model_accuracy': full_accuracy,
        'top5_model_accuracy': top_accuracy
    }
