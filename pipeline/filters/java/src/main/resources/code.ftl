package com.dtstack.jlogstash.filters;

import java.util.*;
import java.lang.*;

public class JavaCodeUtil {

    public Map filter(Map event) {
        ${code}
        return event;
    }
}