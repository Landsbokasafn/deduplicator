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

public interface DeDuplicatorConstants {
    public static final String REVISIT_ANNOTATION_MARKER="Revisit:IdenticalPayloadDigest";
    
    /* Extra info for crawl log, JSON keys */
    public static final String EXTRA_REVISIT_PROFILE="RevisitProfile";
    public static final String EXTRA_REVISIT_URI="RevisitRefersToURI";
    public static final String EXTRA_REVISIT_DATE="RevisitRefersToDate";
}
