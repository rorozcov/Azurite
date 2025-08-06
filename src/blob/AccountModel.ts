export interface AccountModel {
  key: "account"; // This is to force loki to treat this as a singleton
  isBlobVersioningEnabled: boolean;
}
