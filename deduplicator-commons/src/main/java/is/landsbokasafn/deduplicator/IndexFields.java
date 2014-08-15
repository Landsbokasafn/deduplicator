/* Copyright (C) 2006-2014 National and University Library of Iceland (NULI)
 * 
 * This file is part of the DeDuplicator (Heritrix add-on module).
 * 
 *  NULI licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package is.landsbokasafn.deduplicator;

/**
 * These enums correspond to the names of fields in the Lucene index
 */
public enum IndexFields {
    /** The URL 
     *  This value is suitable for use in warc/revisit records as the WARC-Refers-To-Target-URI
     **/
	URL,
    /** The content digest as String **/
	DIGEST,
    /** The URLs timestamp (time of fetch). Suitable for use in WARC-Refers-To-Date. Encoded according to
     *  w3c-iso8601  
     */
    DATE,
    /** The document's etag **/
    ETAG,
    /** A canonicalized version of the URL **/
	URL_CANONICALIZED,
    /** WARC Record ID of original payload capture. Suitable for WARC-Refers-To field. **/
    ORIGINAL_RECORD_ID;

}
