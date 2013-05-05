/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.ldbc;

import java.util.Map;

/**
 * Creates a DB layer by dynamically class loading the specified DB class
 */
public class DBFactory
{
    public DBFactory()
    {

    }

    public DB newDB( String dbClassName, Map<String, String> properties ) throws UnknownDBException
    {
        try
        {
            ClassLoader classLoader = DBFactory.class.getClassLoader();
            Class<? extends DB> dbclass = (Class<? extends DB>) classLoader.loadClass( dbClassName );
            DB db = (DB) dbclass.newInstance();
            db.setProperties( properties );
            return new DBWrapper( db );
        }
        catch ( InstantiationException e )
        {
            throw new UnknownDBException( "Error creating DB from dynamically loaded class", e.getCause() );
        }
        catch ( IllegalAccessException e )
        {
            throw new UnknownDBException( "Error creating DB from dynamically loaded class", e.getCause() );
        }
        catch ( ClassNotFoundException e )
        {
            throw new UnknownDBException( "Error creating DB from dynamically loaded class", e.getCause() );
        }
    }

}
