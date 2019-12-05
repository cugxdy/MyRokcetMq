/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.filtersrv;

import org.slf4j.Logger;

public class FilterServerUtil {
	
    public static void callShell(final String shellString, final Logger log) {
        Process process = null;
        try {
        	// 以" "分隔字符串对象,数组形式
            String[] cmdArray = splitShellString(shellString);
            // 运行命令行程序
            process = Runtime.getRuntime().exec(cmdArray);
            
            // Causes the current thread to wait, 
            // if necessary, until theprocess represented by this Process object hasterminated. 
            // This method returns immediately if the subprocesshas already terminated. 
            // If the subprocess has not yetterminated, 
            // the calling thread will be blocked until thesubprocess exits.
            process.waitFor();
            log.info("CallShell: <{}> OK", shellString);
        } catch (Throwable e) {
            log.error("CallShell: readLine IOException, {}", shellString, e);
        } finally {
            if (null != process)
            	// 关闭process对象
                process.destroy();
        }
    }

    private static String[] splitShellString(final String shellString) {
        return shellString.split(" ");
    }
}
