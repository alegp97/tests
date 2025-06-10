@SuppressWarnings("unchecked")
private static void deepMerge(Map<String, Object> lowPriorityMap, Map<String, Object> highPriorityMap) {
    for (String key : highPriorityMap.keySet()) {
        Object highPriorityProperty = highPriorityMap.get(key);  // ← el "nuevo" alto
        Object lowPriorityProperty = lowPriorityMap.get(key);    // ← el "nuevo" bajo

        if (lowPriorityProperty instanceof Map && highPriorityProperty instanceof Map) {
            deepMerge((Map<String, Object>) lowPriorityProperty, (Map<String, Object>) highPriorityProperty);
        } else if (lowPriorityProperty instanceof Collection && highPriorityProperty instanceof Collection) {
            ((Collection<Object>) lowPriorityProperty).addAll((Collection<?>) highPriorityProperty);
        } else {
            lowPriorityMap.put(key, highPriorityProperty);
        }
    }
}
