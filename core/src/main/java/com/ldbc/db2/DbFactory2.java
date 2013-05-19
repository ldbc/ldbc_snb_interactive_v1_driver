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

package com.ldbc.db2;

import java.util.Map;

/**
 * Creates a DB layer by dynamically class loading the specified DB class
 */
public class DbFactory2
{
    public Db2 newDb( String dbClassName, Map<String, String> properties ) throws UnknownDBException2
    {
        try
        {
            ClassLoader classLoader = DbFactory2.class.getClassLoader();
            Class<? extends Db2> dbclass = (Class<? extends Db2>) classLoader.loadClass( dbClassName );
            Db2 db = (Db2) dbclass.newInstance();
            return db;
        }
        catch ( InstantiationException e )
        {
            throw new UnknownDBException2( "Error creating DB from dynamically loaded class", e.getCause() );
        }
        catch ( IllegalAccessException e )
        {
            throw new UnknownDBException2( "Error creating DB from dynamically loaded class", e.getCause() );
        }
        catch ( ClassNotFoundException e )
        {
            throw new UnknownDBException2( "Error creating DB from dynamically loaded class", e.getCause() );
        }
    }

}
