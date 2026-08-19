export type LaunchSpec = {
    schema_version: 1;
    image: string;
    command: readonly string[];
    flavor: string;
    timeout: string;
    environment: Readonly<Record<string, string>>;
    secret_names: readonly string[];
    labels: Readonly<Record<string, string>>;
    namespace?: string;
};
export declare function parseLaunchSpec(value: unknown): LaunchSpec;
export declare function launchSpecSha256(spec: LaunchSpec): string;
//# sourceMappingURL=launch-spec.d.ts.map