package org.apache.flink;

import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.MemberSubstitution;
import net.bytebuddy.description.NamedElement;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.util.LinkedHashMap;
import java.util.Map;

public class MethodReplacerAgent {
    private static final Logger LOG = LoggerFactory.getLogger(MethodReplacerAgent.class);

    public static void premain(String args, Instrumentation inst) throws NoSuchMethodException {
        AgentConfig config = AgentConfig.parse(args);
        if (!config.hasTransformConfig()) {
            LOG.info("MethodReplacerAgent has no configured targets or methods; skip agent transformation.");
            return;
        }
        loadNativeLibrary();
        LOG.info(
                "MethodReplacerAgent config: targets={}, replaceMethods={}",
                join(config.targetClasses),
                join(config.replaceMethods));

        MethodDescription replaceMethod =
                new MethodDescription.ForLoadedMethod(
                        ReplaceHelper.class.getMethod("replaceAllFast", String.class, String.class, String.class)
                );

        // CRC32: in Utils class, replace Crc32.crc32(byte[],int,int) -> Crc32Helper.crc32Fast
        MethodDescription crc32Method =
                new MethodDescription.ForLoadedMethod(
                        Crc32Helper.class.getMethod("crc32Fast", byte[].class, int.class, int.class)
        );

        new AgentBuilder.Default()
                .type(namedAny(config.targetClasses))
                .transform((builder, typeDescription, classLoader, module, protectionDomain) ->
                        builder.visit(
                                MemberSubstitution.strict()
                                        .method(
                                                ElementMatchers.named("crc32")
                                                        .and(ElementMatchers.takesArguments(byte[].class, int.class, int.class))
                                                        .and(ElementMatchers.isStatic())
                                                        .and(ElementMatchers.isDeclaredBy(
                                                                ElementMatchers.named("org.example.Crc32")))
                                        )
                                        .replaceWith(crc32Method)
                                        .on(ElementMatchers.any())
                        )
                )
                .transform((builder, typeDescription, classLoader, module, protectionDomain) -> {
                            LOG.info("transform running for {}", typeDescription.getName());
                            // replaceAll
                            if (config.replaceMethods.length > 0) {
                                builder = builder.visit(
                                        MemberSubstitution.strict()
                                                .method(
                                                        ElementMatchers.named("replaceAll")
                                                                .and(ElementMatchers.takesArguments(String.class, String.class))
                                                                .and(ElementMatchers.isDeclaredBy(String.class))
                                                )
                                                .replaceWith(replaceMethod)
                                                .on(namedAny(config.replaceMethods))
                                );
                            }
                            return builder;
                        }
                )
                .installOn(inst);

    }

    private static void loadNativeLibrary() {
        try {
            System.loadLibrary("regex");
            LOG.info("Successfully loaded native library regex");
        } catch (Exception e) {
            LOG.error("Native code library failed to load regex");
        }
    }

    private static ElementMatcher.Junction<NamedElement> namedAny(String[] names) {
        ElementMatcher.Junction<NamedElement> matcher = ElementMatchers.none();
        for (String name : names) {
            matcher = matcher.or(ElementMatchers.named(name));
        }
        return matcher;
    }

    private static String[] configuredValues(
            Map<String, String> agentArgs, String agentKey, String systemPropertyKey) {
        String configured = agentArgs.get(agentKey);
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getProperty(systemPropertyKey);
        }
        return splitCsv(configured);
    }

    private static String[] splitCsv(String value) {
        if (value == null || value.trim().isEmpty()) {
            return new String[0];
        }
        String[] parts = value.split(",");
        int count = 0;
        for (String part : parts) {
            if (!part.trim().isEmpty()) {
                count++;
            }
        }
        String[] values = new String[count];
        int index = 0;
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values[index++] = trimmed;
            }
        }
        return values;
    }

    private static String join(String[] values) {
        if (values.length == 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(values[0]);
        for (int i = 1; i < values.length; i++) {
            builder.append(",").append(values[i]);
        }
        return builder.toString();
    }

    private static final class AgentConfig {
        private final String[] targetClasses;
        private final String[] replaceMethods;
        private final String[] lowerMethods;

        private AgentConfig(String[] targetClasses, String[] replaceMethods, String[] lowerMethods) {
            this.targetClasses = targetClasses;
            this.replaceMethods = replaceMethods;
            this.lowerMethods = lowerMethods;
        }

        private static AgentConfig parse(String args) {
            Map<String, String> agentArgs = parseAgentArgs(args);
            return new AgentConfig(
                    configuredValues(agentArgs, "targets", "agent.targets"),
                    configuredValues(agentArgs, "replaceMethods", "agent.replaceMethods"));
        }

        private boolean hasTransformConfig() {
            return targetClasses.length > 0 && (replaceMethods.length > 0 || lowerMethods.length > 0);
        }

        private static Map<String, String> parseAgentArgs(String args) {
            Map<String, String> parsed = new LinkedHashMap<>();
            if (args == null || args.trim().isEmpty()) {
                return parsed;
            }
            String[] entries = args.split(";");
            for (String entry : entries) {
                int separator = entry.indexOf('=');
                if (separator <= 0) {
                    continue;
                }
                String key = entry.substring(0, separator).trim();
                String value = entry.substring(separator + 1).trim();
                if (!key.isEmpty()) {
                    parsed.put(key, value);
                }
            }
            return parsed;
        }
    }
}
