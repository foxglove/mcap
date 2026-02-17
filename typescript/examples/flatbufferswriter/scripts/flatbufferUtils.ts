import type {
  Grid,
  FrameTransform,
  Quaternion,
  Vector3,
  PackedElementField,
  Pose,
  Vector2,
} from "@foxglove/schemas";
import { Builder } from "flatbuffers";

import {
  FrameTransform as FbFrameTransform,
  Quaternion as FbQuaternion,
  Time as FbTime,
  Vector3 as FbVector3,
} from "../../flatbuffer/output/FrameTransform_generated.ts";
import {
  PackedElementField as FbPackedElementField,
  Pose as FbPose,
  Grid as FbGrid,
  Vector2 as FbVector2,
} from "../../flatbuffer/output/Grid_generated.ts";

export function buildQuaternion(builder: Builder, quatJson: Quaternion): number {
  FbQuaternion.startQuaternion(builder);
  FbQuaternion.addX(builder, quatJson.x);
  FbQuaternion.addY(builder, quatJson.y);
  FbQuaternion.addZ(builder, quatJson.z);
  FbQuaternion.addW(builder, quatJson.w);
  return FbQuaternion.endQuaternion(builder);
}

export function buildVector2(builder: Builder, json: Vector2): number {
  FbVector2.startVector2(builder);
  FbVector2.addX(builder, json.x);
  FbVector2.addY(builder, json.y);
  return FbVector2.endVector2(builder);
}

export function buildVector3(builder: Builder, json: Vector3): number {
  FbVector3.startVector3(builder);
  FbVector3.addX(builder, json.x);
  FbVector3.addY(builder, json.y);
  FbVector3.addZ(builder, json.z);
  return FbVector3.endVector3(builder);
}

export function buildPose(builder: Builder, json: Pose): number {
  const pos = buildVector3(builder, json.position);
  const quat = buildQuaternion(builder, json.orientation);
  FbPose.startPose(builder);
  FbPose.addOrientation(builder, quat);
  FbPose.addPosition(builder, pos);
  return FbPose.endPose(builder);
}

export function buildTfMessage(builder: Builder, tfJson: FrameTransform): number {
  const parentFrameId = builder.createString(tfJson.parent_frame_id);
  const childFrameId = builder.createString(tfJson.child_frame_id);

  const quat = buildQuaternion(builder, tfJson.rotation);

  const vec3 = buildVector3(builder, tfJson.translation);

  FbFrameTransform.startFrameTransform(builder);
  FbFrameTransform.addParentFrameId(builder, parentFrameId);
  FbFrameTransform.addChildFrameId(builder, childFrameId);
  FbFrameTransform.addTranslation(builder, vec3);
  FbFrameTransform.addRotation(builder, quat);
  FbFrameTransform.addTimestamp(
    builder,
    FbTime.createTime(builder, BigInt(tfJson.timestamp.sec), tfJson.timestamp.nsec),
  );

  const tf = FbFrameTransform.endFrameTransform(builder);
  return tf;
}

export function buildPackedElementField(builder: Builder, json: PackedElementField): number {
  const name = builder.createString(json.name);
  FbPackedElementField.startPackedElementField(builder);
  FbPackedElementField.addName(builder, name);
  FbPackedElementField.addOffset(builder, json.offset);
  FbPackedElementField.addType(builder, json.type);
  return FbPackedElementField.endPackedElementField(builder);
}

export function buildGridMessage(builder: Builder, json: Grid): number {
  const frameId = builder.createString(json.frame_id);

  const pose = buildPose(builder, json.pose);

  const cellSize = buildVector2(builder, json.cell_size);
  const data = FbGrid.createDataVector(builder, json.data);

  const fbFields = [];
  for (const field of json.fields) {
    fbFields.push(buildPackedElementField(builder, field));
  }
  const fieldVector = FbGrid.createFieldsVector(builder, fbFields);

  FbGrid.startGrid(builder);
  FbGrid.addTimestamp(
    builder,
    FbTime.createTime(builder, BigInt(json.timestamp.sec), json.timestamp.nsec),
  );
  FbGrid.addFrameId(builder, frameId);
  FbGrid.addPose(builder, pose);
  FbGrid.addCellSize(builder, cellSize);
  FbGrid.addCellStride(builder, json.cell_stride);
  FbGrid.addColumnCount(builder, json.column_count);
  FbGrid.addRowStride(builder, json.row_stride);
  FbGrid.addFields(builder, fieldVector);
  FbGrid.addData(builder, data);

  return FbGrid.endGrid(builder);
}
