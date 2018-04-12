/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.bad;

import java.util.logging.Logger;


/**
 * Provides functionality for channel jobs
 */
public class ChannelJobService {

    private static final Logger LOGGER = Logger.getLogger(ChannelJobService.class.getName());


    public static long findPeriod(String duration) {
        //TODO: Allow Repetitive Channels to use YMD durations
        String hoursMinutesSeconds = "";
        if (duration.indexOf('T') != -1) {
            hoursMinutesSeconds = duration.substring(duration.indexOf('T') + 1);
        }
        double seconds = 0;
        if (hoursMinutesSeconds != "") {
            int pos = 0;
            if (hoursMinutesSeconds.indexOf('H') != -1) {
                Double hours = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('H')));
                seconds += (hours * 60 * 60);
                pos = hoursMinutesSeconds.indexOf('H') + 1;
            }
            if (hoursMinutesSeconds.indexOf('M') != -1) {
                Double minutes =
                        Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('M')));
                seconds += (minutes * 60);
                pos = hoursMinutesSeconds.indexOf('M') + 1;
            }
            if (hoursMinutesSeconds.indexOf('S') != -1) {
                Double s = Double.parseDouble(hoursMinutesSeconds.substring(pos, hoursMinutesSeconds.indexOf('S')));
                seconds += (s);
            }
        }
        return (long) (seconds * 1000);
    }


    @Override
    public String toString() {
        return "ChannelJobService";
    }

}
