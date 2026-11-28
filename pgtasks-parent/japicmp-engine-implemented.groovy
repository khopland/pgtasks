// japicmp's post-analysis script; see the japicmp plugin in pom.xml.
//
// The engine implements these interfaces and applications only call them, so a minor release may
// add methods to them: japicmp would report each new method as a source break for whoever
// implements the interface, and nobody outside the engine does. Every other change to them, a
// removed or changed method included, is still reported and still fails the build.
import japicmp.model.JApiCompatibilityChangeType

def engineImplemented = [
    'io.github.khopland.pgtasks.engine.extension.ExtensionContext',
    'io.github.khopland.pgtasks.engine.extension.ExtensionContext$Job',
    'io.github.khopland.pgtasks.engine.extension.ExtensionContext$Channel',
    'io.github.khopland.pgtasks.engine.extension.ExtensionData',
] as Set

jApiClasses.findAll { engineImplemented.contains(it.fullyQualifiedName) }.each { jApiClass ->
    (jApiClass.methods*.compatibilityChanges + [jApiClass.compatibilityChanges]).flatten()
        .findAll { it.type == JApiCompatibilityChangeType.METHOD_ADDED_TO_INTERFACE }
        .each { it.sourceCompatible = true }
}
return jApiClasses
