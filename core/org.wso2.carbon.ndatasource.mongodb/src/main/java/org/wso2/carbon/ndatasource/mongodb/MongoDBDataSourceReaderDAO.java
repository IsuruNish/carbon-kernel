package org.wso2.carbon.ndatasource.mongodb;

import org.wso2.carbon.ndatasource.common.DataSourceException;

public interface MongoDBDataSourceReaderDAO {

    public String getType();
    public Object createDataSource(String xmlConfiguration, boolean isDataSourceFactoryReference) throws DataSourceException;
    public boolean testDataSourceConnection(String xmlConfiguration) throws DataSourceException;
}
