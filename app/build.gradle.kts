plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
}

android {
    namespace = "com.pulse.agent"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.pulse.agent"
        // 无障碍 takeScreenshot 与 OCR 需要 API 30（见 asset-audit §3 工程项）。
        minSdk = 30
        targetSdk = 36
        versionCode = 1
        versionName = "0.1.0"
        buildConfigField("int", "PROTOCOL_VERSION", "1")
    }

    buildFeatures {
        buildConfig = true
    }

    buildTypes {
        release {
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            // 正式签名仅在本仓 CI 注入（implementation-plan §6），此处不配置 debug 证书。
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    lint {
        warningsAsErrors = false
        abortOnError = true
    }
}

kotlin {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    }
}

dependencies {
    implementation(libs.gson)
    // Bundled Chinese + Latin recognition; no network model download or INTERNET permission.
    implementation(libs.mlkit.chinese)
    testImplementation(libs.junit)
}
