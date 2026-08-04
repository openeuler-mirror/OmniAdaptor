# Java Agent JVM 参数配置说明

Java Agent 通过 JVM 系统属性`-Dagent.xxx` 配置拦截范围。
如果没有配置目标类，或者没有配置任何需要替换的目标方法，agent 会直接跳过，不进行任何拦截替换，也不会加载 native 库。

## 参数列表

| JVM 系统属性 | 说明                            |
| --- |-------------------------------|
| `agent.targets` | 配置需要被转换的目标类，使用类的全限定名。         |
| `agent.replaceMethods` | 配置第一类（replaceAll）替换逻辑生效的目标方法。 |
| `agent.lowerMethods` | 配置第二类（toLower）替换逻辑生效的目标方法。    |

这三个参数都支持配置一个或多个值。配置多个值时，使用英文逗号 `,` 分隔。值前后的空格会被自动忽略。

## 配置示例

配置多个目标类，并为两类替换逻辑分别配置多个目标方法：

```bash
-Dagent.targets=<目标类全限定名1>,<目标类全限定名2>,<目标类全限定名3>
-Dagent.replaceMethods=<目标方法名1>,<目标方法名2>
-Dagent.lowerMethods=<目标方法名3>,<目标方法名4>
```

## Flink 配置示例

可以在 `flink-conf.yaml` 中通过 `env.java.opts` 或其他flink提供的JVM配置参数进行配置：

```yaml
env.java.opts: "-javaagent:/opt/flink-tnel-0.1-SNAPSHOT.jar 
-Dagent.targets=com.demo.pipeline.CleanJob 
-Dagent.replaceMethods=cleanPayload 
-Dagent.lowerMethods=buildIndex"
```
上述配置将会把 `CleanJob` 类中的 `cleanPayload` 方法调用的 `replaceAll` 方法进行替换。把`CleanJob` 类中的 `buildIndex` 方法调用的 `toLowerCase` 方法进行替换。