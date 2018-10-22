/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.jlogstash.jdistributed.gclog;

import com.dtstack.jlogstash.distributed.logmerge.ClusterLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FIXME 是否需要考虑在一个CMS日志信息中夹杂了其他CMS日志信息
 * 检查队列数据整理出GC-CMS日志记录
 * Date: 2016/12/28
 * Company: www.dtstack.com
 *
 * @ahthor xuchao
 */
public class CMSLogPattern {

    private static final Logger logger = LoggerFactory.getLogger(CMSLogPattern.class);

    public static int MERGE_NUM = 12;

    public static final Pattern full_gc = Pattern.compile(".*CMS.*");

    public static final String heap_size_re = "([0-9]+)([KM])";

    public static final String gc_time_re = "([0-9]+\\.[0-9]+)";

    public static final String heap_size_paren_re = "\\(" + heap_size_re + "\\)";

    public static final String cms_heap_size_re = heap_size_re + heap_size_paren_re;

    public static final String gc_time_secs_re = gc_time_re + " (secs)";

    public static final String cms_heap_report_re = cms_heap_size_re + ", " + gc_time_secs_re;

    public static final String timestamp_re = "(" + gc_time_re + ": *)?";

    private static Pattern _cms_imark_pattern = Pattern.compile(
                timestamp_re + "\\[GC\\s*(\\(CMS\\s*Initial\\s*Mark\\))?\\s*\\[1\\s*(AS)?CMS-initial-mark:\\s*" +
                cms_heap_size_re + "\\] " +
                cms_heap_report_re + "\\]");

    //(AS)?CMS-concurrent-(mark|(abortable-)?preclean|sweep|reset)
    private static Pattern conmark_beg_pattern = Pattern.compile(timestamp_re + "\\[(AS)?CMS-concurrent-mark-start\\]");

    private static Pattern conmark_phase_pattern = Pattern.compile(
            timestamp_re + "\\[(AS)?CMS-concurrent-mark:\\s*" + gc_time_re + "/" + gc_time_secs_re + "\\]");

    private static Pattern conpreclean_beg_pattern = Pattern.compile(timestamp_re + "\\[(AS)?CMS-concurrent-preclean-start\\]");

    private static Pattern conpreclean_phase_pattern = Pattern.compile(
            timestamp_re + "\\[(AS)?CMS-concurrent-preclean:\\s*" + gc_time_re + "/" + gc_time_secs_re + "\\]");

    private static Pattern conabortable_preclean_beg_pattern = Pattern.compile(timestamp_re + "\\[(AS)?CMS-concurrent-abortable-preclean-start\\]");

    private static Pattern conabortable_preclean_phase_pattern = Pattern.compile(
            timestamp_re + "\\[(AS)?CMS-concurrent-abortable-preclean:\\s*" + gc_time_re + "/" + gc_time_secs_re + "\\]");

    private static Pattern final_remark_pattern = Pattern.compile(
            timestamp_re + "\\[GC.*\\[1\\s*(AS)?CMS-remark:\\s*" +
                    cms_heap_size_re + "\\] " +
                    cms_heap_report_re + "\\]");

    private static Pattern consweep_beg_pattern = Pattern.compile(timestamp_re + "\\[(AS)?CMS-concurrent-sweep-start\\]");

    private static Pattern consweep_phase_pattern = Pattern.compile(
            timestamp_re + "\\[(AS)?CMS-concurrent-sweep:\\s*" + gc_time_re + "/" + gc_time_secs_re + "\\]");


    private static Pattern conreset_beg_pattern = Pattern.compile(timestamp_re + "\\[(AS)?CMS-concurrent-reset-start\\]");

    private static Pattern conreset_phase_pattern = Pattern.compile(
            timestamp_re + "\\[(AS)?CMS-concurrent-reset:\\s*" + gc_time_re + "/" + gc_time_secs_re + "\\]");


    private static String young_gc_re = "(([0-9]+\\.[0-9]+):*)?\\s*\\[GC\\s*\\(.*\\).*(([0-9]+\\.[0-9]+):*)?" +
            "\\[(DefNew|(AS)?ParNew)(.*\\r?\\n)*(-\\s*age\\s*([0-9]):\\s*[0-9]+\\s*bytes,\\s*" +
            "[0-9]+\\s*total\\r?\\n)*:\\s*([0-9]+)([KM])->([0-9]+)([KM])?\\(([0-9]+)([KM])\\)," +
            "\\s*([0-9]+\\.[0-9]+)\\s*(secs)\\]\\s*([0-9]+)([KM])->([0-9]+)([KM])?\\(([0-9]+)([KM])\\)," +
            "\\s*([0-9]+\\.[0-9]+)\\s*(secs)\\]\\s*\\[Times:\\s*user=([0-9]+\\.[0-9]+)\\s*" +
            "sys=([0-9]+\\.[0-9]+),\\s*real=([0-9]+\\.[0-9]+)\\s*(secs)\\]";

    private static Pattern young_gc_pattern = Pattern.compile(young_gc_re);

    private static Pattern gc_begin_pattern = Pattern.compile("CommandLine\\s*flags");

    public boolean checkIsYoungGC(String log){
        Matcher matcher = young_gc_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    public boolean checkIsGCBegin(String log){
        Matcher matcher = gc_begin_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 判断是不是cms的full gc
     * @param log
     * @return
     */
    public boolean checkIsFullGC(String log){
        Matcher matcher = full_gc.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 校验是不是一条完整的cms full gc日志
     * @return
     */
    public boolean checkIsCompleteLog(List<ClusterLog> logPool, int startIndex){
        if (logPool.size() < startIndex + CMSLogPattern.MERGE_NUM){
            logger.info("log size is not match cms step.");
            return false;
        }

        String initMarkLog = logPool.get(0 + startIndex).getLoginfo();
        if(!checkInitialMark(initMarkLog)){
            logger.debug("----not match init mark-----,msg:{}", initMarkLog);
            return false;
        }

        String conMarkStartLog = logPool.get(1 + startIndex).getLoginfo();
        if(!checkConMarkStart(conMarkStartLog)){
            logger.debug("----not match conMarkStartLog-----,msg:{}", conMarkStartLog);
            return false;
        }

        String conMarkPhaseLog = logPool.get(2 + startIndex).getLoginfo();
        if(!checkConMarkPhase(conMarkPhaseLog)){
            logger.debug("----not match conMarkPhaseLog-----,msg:{}", conMarkPhaseLog);
            return false;
        }

        String conPreCleanStartLog = logPool.get(3 + startIndex).getLoginfo();
        if(!checkConPrecleanStart(conPreCleanStartLog)){
            logger.debug("----not match conPreCleanStartLog-----,msg:{}", conPreCleanStartLog);
            return false;
        }

        String conPreCleanPhaseLog = logPool.get(4 + startIndex).getLoginfo();
        if(!checkConPrecleanPhase(conPreCleanPhaseLog)){
            logger.debug("----not match conPreCleanPhaseLog-----,msg:{}", conPreCleanPhaseLog);
            return false;
        }

        String abortablePrecleanStartLog = logPool.get(5 + startIndex).getLoginfo();
        if(!checkAbortablePrecleanStart(abortablePrecleanStartLog)){
            logger.debug("----not match abortablePrecleanStartLog-----,msg:{}", abortablePrecleanStartLog);
            return false;
        }

        String abortablePrecleanPhaseLog = logPool.get(6 + startIndex).getLoginfo();
        if(!checkAbortablePrecleanPhase(abortablePrecleanPhaseLog)){
            logger.debug("----not match abortablePrecleanPhaseLog-----,msg:{}", abortablePrecleanPhaseLog);
            return false;
        }

        String finalRemarkLog = logPool.get(7 + startIndex).getLoginfo();
        if(!checkFinalRemark(finalRemarkLog)){
            logger.debug("----not match finalRemarkLog-----,msg:{}", finalRemarkLog);
            return false;
        }

        String conSweepStartLog = logPool.get(8 + startIndex).getLoginfo();
        if(!checkConSweepStart(conSweepStartLog)){
            logger.debug("----not match conSweepStartLog-----,msg:{}", conSweepStartLog);
            return false;
        }

        String conSweepPhaseLog = logPool.get(9 + startIndex).getLoginfo();
        if(!checkConSweepPhase(conSweepPhaseLog)){
            logger.debug("----not match conSweepPhaseLog-----,msg:{}", conSweepPhaseLog);
            return false;
        }

        String conResetStartLog = logPool.get(10 + startIndex).getLoginfo();
        if(!checkConResetStart(conResetStartLog)){
            logger.debug("----not match conResetStartLog-----,msg:{}", conResetStartLog);
            return false;
        }

        String conResetPhaseLog = logPool.get(11 + startIndex).getLoginfo();
        if(!checkConResetPhase(conResetPhaseLog)){
            logger.debug("----not match conResetPhaseLog-----,msg:{}", conResetPhaseLog);
            return false;
        }

        logger.debug("--success of cms msg----");
        return true;
    }



    //FIXME CMS清理过程是会被打断的

    /**
     * 2016-12-28T10:07:21.971+0800: 1190255.3662016-12-28T10:07:20.994+0800: 1190254.390: [GC (CMS Initial Mark) [1 CMS-initial-mark: 2786997K(3670016K)] 2839212K(4141888K), 0.0059182 secs] [Times: user=0.02 sys=0.00, real=0.01 secs]
     */
    public boolean checkInitialMark(String log){//stop the world
        Matcher matcher = _cms_imark_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    /**
     * 2016-12-28T10:07:21.000+0800: 1190254.396: [CMS-concurrent-mark-start]
     */
    private boolean checkConMarkStart(String log){
        Matcher matcher = conmark_beg_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-mark: 0.970/0.970 secs] [Times: user=1.29 sys=0.09, real=0.97 secs]
     * @return
     */
    private boolean checkConMarkPhase(String log){
        Matcher matcher = conmark_phase_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-preclean-start]
     */
    private boolean checkConPrecleanStart(String log){
        Matcher matcher = conpreclean_beg_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    /**
     * 2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-preclean: 0.019/0.020 secs] [Times: user=0.02 sys=0.01, real=0.02 secs]
     * @return
     */
    private boolean checkConPrecleanPhase(String log){
        Matcher matcher = conpreclean_phase_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-abortable-preclean-start]
     */
    private boolean checkAbortablePrecleanStart(String log){
        Matcher matcher = conabortable_preclean_beg_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 2016-12-30T09:44:44.003+0800: 1361697.398: [CMS-concurrent-abortable-preclean: 0.425/0.708 secs] [Times: user=1.75 sys=0.00, real=0.71 secs]
     * @return
     */
    private boolean checkAbortablePrecleanPhase(String log){
        Matcher matcher = conabortable_preclean_phase_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return  false;
    }

    /**
     * 2016-12-28T10:07:27.081+0800: 1190260.476: [GC (CMS Final Remark) [YG occupancy: 150056 K (471872 K)]2016-12-28T10:07:27.081+0800: 1190260.476: [Rescan (parallel) , 0.0174614 secs]2016-12-28T10:07:27.098+0800: 1190260.494: [weak refs processing, 0.0057690 secs]2016-12-28T10:07:27.104+0800: 1190260.499: [class unloading, 0.0364629 secs]2016-12-28T10:07:27.141+0800: 1190260.536: [scrub symbol table, 0.0153273 secs]2016-12-28T10:07:27.156+0800: 1190260.551: [scrub string table, 0.0021518 secs][1 CMS-remark: 2789424K(3670016K)] 2939480K(4141888K), 0.0790679 secs] [Times: user=0.11 sys=0.01, real=0.08 secs]
     */
    private boolean checkFinalRemark(String log){//stop the world
        Matcher matcher = final_remark_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return  false;
    }

    /**
     * 2016-12-28T10:07:27.161+0800: 1190260.557: [CMS-concurrent-sweep-start]
     */
    private boolean checkConSweepStart(String log){
        Matcher matcher = consweep_beg_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }

        return false;
    }

    /**
     * 2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-sweep: 0.738/0.738 secs] [Times: user=1.57 sys=0.00, real=0.74 secs]
     * @return
     */
    private boolean checkConSweepPhase(String log){
        Matcher matcher = consweep_phase_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    /**
     * 2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-reset-start]
     */
    private boolean checkConResetStart(String log){
        Matcher matcher = conreset_beg_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    /**
     * 2016-12-28T10:07:27.908+0800: 1190261.303: [CMS-concurrent-reset: 0.009/0.009 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]
     * @return
     */
    private boolean checkConResetPhase(String log){
        Matcher matcher = conreset_phase_pattern.matcher(log);
        if(matcher.find()){
            return true;
        }
        return false;
    }

    //FIXME TETST
    public static void main(String[] args) {
        CMSLogPattern cmsLogMerge = new CMSLogPattern();
        boolean isInitMark = cmsLogMerge.checkInitialMark("2016-12-28T10:07:21.971+0800: 1190255.3662016-12-28T10:07:20.994+0800: 1190254.390: [GC (CMS Initial Mark) [1 CMS-initial-mark: 2786997K(3670016K)] 2839212K(4141888K), 0.0059182 secs] [Times: user=0.02 sys=0.00, real=0.01 secs]");
        if(isInitMark){
            System.out.println("is init mark");
        }else{
            System.out.println("is not init mark");
        }

        String conMarkStart = "2016-12-28T10:07:21.000+0800: 1190254.396: [CMS-concurrent-mark-start]";
        boolean isconMark = cmsLogMerge.checkConMarkStart(conMarkStart);
        if(isconMark){
            System.out.println("is con mark start");
        }else{
            System.out.println("is not conMark start");
        }

        String conMarkEnd = "2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-mark: 0.970/0.970 secs] [Times: user=1.29 sys=0.09, real=0.97 secs]";
        if(cmsLogMerge.checkConMarkPhase(conMarkEnd)){
            System.out.println("is con mark phase");
        }else{
            System.out.println("is not con mark phase");
        }

        String conPrecleanStart = "2016-12-28T10:07:21.971+0800: 1190255.366: [CMS-concurrent-preclean-start]";
        if (cmsLogMerge.checkConPrecleanStart(conPrecleanStart)){
            System.out.println("is con preclean start");
        }else{
            System.out.println("is not conpreclean start");
        }

        String conPrecleanPhase = "2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-preclean: 0.019/0.020 secs] [Times: user=0.02 sys=0.01, real=0.02 secs]";
        if(cmsLogMerge.checkConPrecleanPhase(conPrecleanPhase)){
            System.out.println("is con preclean phase");
        }else{
            System.out.println("is not con preclean phase");
        }


        String conAbordCleanStart = "2016-12-28T10:07:21.991+0800: 1190255.387: [CMS-concurrent-abortable-preclean-start]";
        if(cmsLogMerge.checkAbortablePrecleanStart(conAbordCleanStart)){
            System.out.println("is con abord clean start.");
        }else{
            System.out.println("is not con abord clean start.");
        }

        String conAbordCleanPhase = "CMS: abort preclean due to time 2016-12-28T10:07:27.079+0800: 1190260.474: [CMS-concurrent-abortable-preclean: 2.646/5.088 secs] [Times: user=4.50 sys=0.25, real=5.09 secs]";
        if(cmsLogMerge.checkAbortablePrecleanPhase(conAbordCleanPhase)){
            System.out.println("is con abord clean phase");
        }else{
            System.out.println("is not con abord clean phase");
        }

        String finalRemark = "2016-12-28T10:07:27.081+0800: 1190260.476: [GC (CMS Final Remark) [YG occupancy: 150056 K (471872 K)]2016-12-28T10:07:27.081+0800: 1190260.476: [Rescan (parallel) , 0.0174614 secs]2016-12-28T10:07:27.098+0800: 1190260.494: [weak refs processing, 0.0057690 secs]2016-12-28T10:07:27.104+0800: 1190260.499: [class unloading, 0.0364629 secs]2016-12-28T10:07:27.141+0800: 1190260.536: [scrub symbol table, 0.0153273 secs]2016-12-28T10:07:27.156+0800: 1190260.551: [scrub string table, 0.0021518 secs][1 CMS-remark: 2789424K(3670016K)] 2939480K(4141888K), 0.0790679 secs] [Times: user=0.11 sys=0.01, real=0.08 secs]";
        if(cmsLogMerge.checkFinalRemark(finalRemark)){
            System.out.println("is final remark");
        }else{
            System.out.println("is not final remark");
        }

        String conSweepStart = "2016-12-28T10:07:27.161+0800: 1190260.557: [CMS-concurrent-sweep-start]";
        if(cmsLogMerge.checkConSweepStart(conSweepStart)){
            System.out.println("is con sweep start.");
        }else{
            System.out.println("is not con sweep start.");
        }

        String conSweepPhase = "2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-sweep: 0.738/0.738 secs] [Times: user=1.57 sys=0.00, real=0.74 secs]";
        if(cmsLogMerge.checkConSweepPhase(conSweepPhase)){
            System.out.println("is con sweep phase.");
        }else{
            System.out.println("is not con sweep phase.");
        }

        String conResetStart = "2016-12-28T10:07:27.899+0800: 1190261.294: [CMS-concurrent-reset-start]";
        if(cmsLogMerge.checkConResetStart(conResetStart)){
            System.out.println("is con reset start.");
        }else{
            System.out.println("is not con reset start.");
        }

        String conResetPhase = "2016-12-28T10:07:27.908+0800: 1190261.303: [CMS-concurrent-reset: 0.009/0.009 secs] [Times: user=0.00 sys=0.00, real=0.00 secs]";
        if(cmsLogMerge.checkConResetPhase(conResetPhase)){
            System.out.println("is con reset phase.");
        }else{
            System.out.println("is not con reset phase.");
        }



    }

}
