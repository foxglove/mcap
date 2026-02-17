import { PointsAnnotationType } from "@foxglove/schemas";
import type {
  CameraCalibration,
  ImageAnnotations,
  PointsAnnotation,
  RawImage,
  Time,
} from "@foxglove/schemas";

type RawImageJson = Omit<RawImage, "data"> & { data: string };

type SceneParams = {
  width: number;
  height: number;
  ballRadius: number;
  gravityCoefficient: number;
  frameId: string;
};

type Ball = {
  pos: { x: number; y: number }; // 0-1
  vel: { x: number; y: number }; // 0-1
};

export default class Scene {
  public image: Image;
  public width: number;
  public height: number;
  public aspect: number;
  public ballRadius: number;
  public gravityCoefficient: number;
  public frameId: string;

  #ball: Ball;

  constructor({ width, height, ballRadius, gravityCoefficient = 0.005, frameId }: SceneParams) {
    this.image = new Image(width, height);
    this.width = width;
    this.height = height;
    this.aspect = width / height;
    this.ballRadius = ballRadius;
    this.gravityCoefficient = gravityCoefficient;
    this.frameId = frameId;
    this.#ball = {
      pos: { x: 0.25, y: 0.5 },
      vel: { x: 0.1, y: 0.1 },
    };
  }

  public getCameraCalibration(time: Time): CameraCalibration {
    const fx = 500;
    const fy = 500;
    const cx = this.width / 2;
    const cy = this.height / 2;
    const calibrationMessage: CameraCalibration = {
      timestamp: time,
      frame_id: "cam",
      height: this.height,
      width: this.width,
      distortion_model: "rational_polynomial",
      D: [],
      K: [fx, 0, cx, 0, fy, cy, 0, 0, 1],
      R: [1, 0, 0, 0, 1, 0, 0, 0, 1],
      P: [fx, 0, cx, 0, 0, fy, cy, 0, 0, 0, 1, 0],
    };

    return calibrationMessage;
  }

  getRawImage(time: Time): RawImageJson {
    return {
      timestamp: time,
      frame_id: this.frameId,
      width: this.width,
      height: this.height,
      encoding: "rgb8",
      step: this.width * 3,
      data: Buffer.from(this.image.getData()).toString("base64"),
    };
  }

  public getImageAnnotations(time: Time): ImageAnnotations {
    const ballBoundingPoints: PointsAnnotation = {
      timestamp: time,
      type: PointsAnnotationType.LINE_LOOP,
      points: [
        {
          x: this.#ball.pos.x * this.width - this.ballRadius * 2,
          y: this.#ball.pos.y * this.height - this.ballRadius * 2,
        },
        {
          x: this.#ball.pos.x * this.width + this.ballRadius * 2,
          y: this.#ball.pos.y * this.height - this.ballRadius * 2,
        },
        {
          x: this.#ball.pos.x * this.width + this.ballRadius * 2,
          y: this.#ball.pos.y * this.height + this.ballRadius * 2,
        },
        {
          x: this.#ball.pos.x * this.width - this.ballRadius * 2,
          y: this.#ball.pos.y * this.height + this.ballRadius * 2,
        },
      ].map((point) => ({ x: Math.floor(point.x), y: Math.floor(point.y) })),
      outline_colors: [],
      thickness: 1,
      outline_color: { r: 0, g: 0, b: 1, a: 1 },
      fill_color: { r: 0, g: 0, b: 1, a: 0.3 },
    };

    return {
      points: [ballBoundingPoints],
      circles: [],
      texts: [
        {
          timestamp: time,
          // top left
          position: {
            x: Math.floor(this.#ball.pos.x * this.width - this.ballRadius * 2),
            y: Math.floor(this.#ball.pos.y * this.height - this.ballRadius * 2),
          },
          text: `Position: x: ${Math.floor(this.#ball.pos.x * this.width)}, y: ${Math.floor(
            this.#ball.pos.y * this.height,
          )}`,
          font_size: 12,
          text_color: { r: 1, g: 1, b: 1, a: 1 },
          background_color: { r: 0, g: 0, b: 1, a: 0.7 },
        },
        {
          timestamp: time,
          position: { x: 15, y: 15 },
          text: `Time: ${time.sec}.${time.nsec}`,
          font_size: 12,
          text_color: { r: 0, g: 0, b: 0, a: 1 },
          background_color: { r: 1, g: 1, b: 1, a: 1 },
        },
      ],
    };
  }

  public animateBall(): void {
    this.#ball.pos.x += this.#ball.vel.x;
    this.#ball.pos.y += this.#ball.vel.y;
    this.#ball.vel.y += this.gravityCoefficient;
    if (this.#ball.pos.x < 0 || this.#ball.pos.x > 1) {
      this.#ball.vel.x *= -0.8;
      this.#ball.pos.x = Math.max(0, Math.min(1, this.#ball.pos.x));
    }
    if (this.#ball.pos.y < 0 || this.#ball.pos.y > 1) {
      this.#ball.vel.y *= -0.8;
      this.#ball.pos.y = Math.max(0, Math.min(1, this.#ball.pos.y));
    }
  }

  public renderScene(): void {
    this.animateBall();
    this.image.clear();
    this.image.paintCircle(
      Math.floor(this.#ball.pos.x * this.width),
      Math.floor(this.#ball.pos.y * this.height),
      5,
      [255, 0, 0],
    );
  }

  public getImageData(): Uint8Array {
    return this.image.getData();
  }
}

// 255
type Color = [number, number, number];

class Image {
  width: number;
  height: number;
  data: Uint8Array;
  constructor(width: number, height: number) {
    this.width = width;
    this.height = height;
    this.data = new Uint8Array(width * height * 3);
  }

  public getData(): Uint8Array {
    return this.data;
  }

  public paintCircle(x: number, y: number, radius: number, color: Color): void {
    const rowRange = [Math.max(0, y - radius), Math.min(this.height - 1, y + radius)] as const;
    const colRange = [Math.max(0, x - radius), Math.min(this.width - 1, x + radius)] as const;
    for (let row = rowRange[0]; row <= rowRange[1]; row++) {
      for (let col = colRange[0]; col <= colRange[1]; col++) {
        const dist = Math.sqrt((row - y) ** 2 + (col - x) ** 2);
        if (dist <= radius * 1.1) {
          const index = (row * this.width + col) * 3;
          this.data[index] = color[0];
          this.data[index + 1] = color[1];
          this.data[index + 2] = color[2];
        }
      }
    }
  }

  public clear(): void {
    for (let i = 0, r = 0; r < this.height; r++) {
      for (let c = 0; c < this.width; c++) {
        if (r === 0 || r === this.height - 1 || c === 0 || c === this.width - 1) {
          this.data[i++] = 150;
          this.data[i++] = 150;
          this.data[i++] = 255;
        } else {
          this.data[i++] =
            (r % Math.floor(this.width * 0.1) === 0 ? 127 : 0) +
            (c % Math.floor(this.height * 0.1) === 0 ? 127 : 0);
          this.data[i++] =
            (r % Math.floor(this.width * 0.1) === 0 ? 127 : 0) +
            (c % Math.floor(this.height * 0.1) === 0 ? 127 : 0);
          this.data[i++] =
            (r % Math.floor(this.width * 0.1) === 0 ? 127 : 0) +
            (c % Math.floor(this.height * 0.1) === 0 ? 127 : 0);
        }
      }
    }
  }
}
