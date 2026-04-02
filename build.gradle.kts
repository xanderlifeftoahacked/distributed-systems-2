plugins {
    java
    application
}

group = "distr"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("info.picocli:picocli:4.7.6")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
}

application {
    mainClass.set("distr.CliMain")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
}

tasks.register<JavaExec>("runCli") {
    group = "application"
    description = "Run CLI entrypoint (distr.CliMain)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("distr.CliMain")
    standardInput = System.`in`
    if (project.hasProperty("args")) {
        args((project.property("args") as String).split(" ").filter { it.isNotBlank() })
    }
}

tasks.register<JavaExec>("runNode") {
    group = "application"
    description = "Run node entrypoint (distr.NodeMain)."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("distr.NodeMain")
    if (project.hasProperty("args")) {
        args((project.property("args") as String).split(" ").filter { it.isNotBlank() })
    }
}

