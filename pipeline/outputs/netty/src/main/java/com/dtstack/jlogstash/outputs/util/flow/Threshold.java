package com.dtstack.jlogstash.outputs.util.flow;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by daguan on 17/6/15.
 */
public class Threshold {

    private Long value;

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    enum Unit {
        PURE_VALUE(Pattern.compile("^([\\.0-9]+)$",Pattern.CASE_INSENSITIVE), 1l),
        B(Pattern.compile("([\\.0-9]+)B",Pattern.CASE_INSENSITIVE), 1l),
        KB(Pattern.compile("([\\.0-9]+)KB",Pattern.CASE_INSENSITIVE), 1l << 10),
        MB(Pattern.compile("([\\.0-9]+)MB",Pattern.CASE_INSENSITIVE), 1l << 20),
        GB(Pattern.compile("([\\.0-9]+)GB",Pattern.CASE_INSENSITIVE), 1l << 30);

        private Pattern pa;
        private long factor;

        public Pattern getPa() {

            return pa;
        }

        public void setPa(Pattern pa) {
            this.pa = pa;
        }

        public long getFactor() {
            return factor;
        }

        public void setFactor(long factor) {
            this.factor = factor;
        }

        Unit(Pattern pa, long factor) {
            this.pa = pa;
            this.factor = factor;
        }

        public static Long validataAndConvert(String value) {
            for(Unit u : Unit.values()) {
                Matcher ma = u.getPa().matcher(value);
                while(ma.find()) {
                    String val = ma.group(1);
                    return u.translate(BigDecimal.valueOf(Double.valueOf(val)));
                }
            }
            return null;
        }

        public boolean validate(String value) {
            return false;
        }

        public long translate(BigDecimal value) {
            return value.multiply(BigDecimal.valueOf(factor)).longValue();
        }

    }

    public Threshold(long threadHold) {
        value = threadHold;
    }


    public static Threshold create(String threadHold) {
        Long v = Unit.validataAndConvert(threadHold.replaceAll(" ",""));
        if(v == null) {
            throw new NullPointerException("threadHold convert to null.");
        }

        return new Threshold(v);
    }

    public static void main(String[] args) {
        System.out.println(Threshold.create(" 1.0 mb").getValue());
    }
}

